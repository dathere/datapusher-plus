# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

import logging
import json
import datetime

from dataclasses import asdict
from dateutil.parser import parse as parse_date
from six.moves.urllib.parse import urljoin  # type: ignore # noqa

import ckan.lib.helpers as h
import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p
from ckan.common import config
import ckanext.datapusher_plus.config as dpp_config
import ckanext.datapusher_plus.logic.schema as dpschema
import ckanext.datapusher_plus.interfaces as interfaces
import ckanext.datapusher_plus.prefect_client as prefect_client
from ckanext.datapusher_plus.utils import utcnow_naive
import ckanext.datapusher_plus.utils as utils
from ckanext.datapusher_plus.jobs.runtime_context import JobInput

from ckanext.datapusher_plus.model import get_job_details

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate

tk = p.toolkit
side_effect_free = logic.side_effect_free

if tk.check_ckan_version("2.10"):
    from ckan.types import Context
    from typing import Any, cast


def _orchestrator():
    """Return the module that runs ingestion jobs for this deployment.

    ``prefect_client`` by default; ``jobs.local_runner`` when
    ``ckanext.datapusher_plus.prefect_enabled`` is false. Both expose
    ``get_running_resource_ids()``, and both have a submit entry point
    (``submit_flow_run`` / ``enqueue_job``) — see ``_submit_job`` for the
    one place the two signatures differ.

    ``local_runner`` is imported lazily (it pulls in every stage class),
    and ``prefect_client`` never imports ``prefect`` at module level —
    so a CKAN process with Prefect turned off gets through this without
    touching Prefect at all. That matters on a host where
    ``$PREFECT_HOME`` is not writable: there, the import itself raises.
    """
    if dpp_config.prefect_enabled():
        return prefect_client

    import ckanext.datapusher_plus.jobs.local_runner as local_runner

    return local_runner


def _submit_job(job_input: JobInput, timeout: int) -> tuple[str, bool]:
    """Hand a job to the active orchestrator.

    Returns ``(run_id, via_prefect)`` — the caller records the id on the
    CKAN ``task_status`` row under the key that matches the backend, so
    ``datapusher_status`` only ever builds a Prefect UI deep-link for a
    run that really is a Prefect flow run.
    """
    orchestrator = _orchestrator()
    if orchestrator is not prefect_client:
        return orchestrator.enqueue_job(job_input, timeout=timeout), False
    return (
        orchestrator.submit_flow_run(asdict(job_input), timeout=timeout),
        True,
    )


def datapusher_submit(context, data_dict: dict[str, Any]):
    """Submit a job to the datapusher. The datapusher is a service that
    imports tabular data into the datastore.

    :param resource_id: The resource id of the resource that the data
        should be imported in. The resource's URL will be used to get the data.
    :type resource_id: string
    :param set_url_type: If set to True, the ``url_type`` of the resource will
        be set to ``datastore`` and the resource URL will automatically point
        to the :ref:`datastore dump <dump>` URL. (optional, default: False)
    :type set_url_type: bool
    :param ignore_hash: If set to True, the datapusher will reload the file
        even if it haven't changed. (optional, default: False)
    :type ignore_hash: bool

    Returns ``True`` if the job has been submitted and ``False`` if the job
    has not been submitted, i.e. when the datapusher is not configured.

    :rtype: bool
    """
    schema = context.get("schema", dpschema.datapusher_submit_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    res_id = data_dict["resource_id"]

    p.toolkit.check_access("datapusher_submit", context, data_dict)

    try:
        resource_dict = p.toolkit.get_action("resource_show")(
            context,
            {
                "id": res_id,
            },
        )
    except logic.NotFound:
        return False

    callback_url_base = config.get("ckan.datapusher.callback_url_base")
    if callback_url_base:
        site_url = callback_url_base
        callback_url = urljoin(
            callback_url_base.rstrip("/"), "/api/3/action/datapusher_hook"
        )
    else:
        site_url = h.url_for("/", qualified=True)
        callback_url = h.url_for("/api/3/action/datapusher_hook", qualified=True)

    for plugin in p.PluginImplementations(interfaces.IDataPusher):
        upload = plugin.can_upload(res_id)
        if not upload:
            msg = "Plugin {0} rejected resource {1}".format(
                plugin.__class__.__name__, res_id
            )
            log.info(msg)
            return False

    task = {
        "entity_id": res_id,
        "entity_type": "resource",
        "task_type": "datapusher_plus",
        "last_updated": utcnow_naive().isoformat(),
        "state": "submitting",
        "key": "datapusher_plus",
        "value": "{}",
        "error": "{}",
    }
    try:
        existing_task = p.toolkit.get_action("task_status_show")(
            context,
            {
                "entity_id": res_id,
                "task_type": "datapusher_plus",
                "key": "datapusher_plus",
            },
        )
        # tk.asint gracefully handles bools/None/whitespace and gives a clean
        # ValueError on garbage — better than bare int() which crashes the
        # action on a malformed config string like "3600s".
        assume_task_stale_after = datetime.timedelta(
            seconds=tk.asint(config.get("ckan.datapusher.assume_task_stale_after", 3600))
        )
        assume_task_stillborn_after = datetime.timedelta(
            seconds=tk.asint(config.get("ckan.datapusher.assume_task_stillborn_after", 5))
        )
        if existing_task.get("state") == "pending":
            # Ask the active orchestrator which resource_ids are still
            # in flight: Prefect's non-terminal flow runs, or — with
            # Prefect disabled — CKAN's RQ queue. Replaces the v2
            # RQ-queue regex scan.
            queued_res_ids = _orchestrator().get_running_resource_ids()
            # Symmetric with the write sites that serialize via
            # ``utcnow_naive().isoformat()`` — ``fromisoformat`` round-trips
            # the same value cleanly and handles the missing-microseconds
            # edge case (which the old ``strptime("%Y-%m-%dT%H:%M:%S.%f")``
            # would raise ``ValueError`` on). Pre-roborev-#2232, the write
            # side used ``str(datetime)`` which produces a space separator
            # instead of ``T`` — that latent format mismatch is also
            # closed by the matched-pair switch.
            updated = datetime.datetime.fromisoformat(
                existing_task["last_updated"]
            )
            time_since_last_updated = utcnow_naive() - updated
            if (
                res_id not in queued_res_ids
                and time_since_last_updated > assume_task_stillborn_after
            ):
                # The resource isn't being processed by any Prefect flow run,
                # and the task_status hasn't been updated past the stillborn
                # threshold — let the new submission restart it.
                log.info(
                    "A pending task was found %r, but it is not in Prefect "
                    "(%r) and is %s old",
                    existing_task["id"],
                    queued_res_ids,
                    time_since_last_updated,
                )
            elif time_since_last_updated > assume_task_stale_after:
                # it's been a while since the job was last updated - it's more
                # likely something went wrong with it and the state wasn't
                # updated than its still in progress. Let it be restarted.
                log.info(
                    "A pending task was found %r, but it is only %s hours" " old",
                    existing_task["id"],
                    time_since_last_updated,
                )
            else:
                log.info(
                    "A pending task was found %s for this resource, so "
                    "skipping this duplicate task",
                    existing_task["id"],
                )
                return False

        task["id"] = existing_task["id"]
    except tk.ObjectNotFound:
        pass

    context["ignore_auth"] = True
    # Use local session for task_status_update, so it can commit its own
    # results without messing up with the parent session that contains pending
    # updats of dataset/resource/etc.
    if tk.check_ckan_version("2.10"):
        context["session"] = cast(Any, context["model"].meta.create_local_session())
    else:
        context["session"] = context["model"].meta.create_local_session()
    tk.get_action("task_status_update")(context, task)

    # This setting is checked on startup
    api_token = utils.get_dp_plus_user_apitoken()
    data = {
        "api_key": api_token,
        "job_type": "push_to_datastore",
        "result_url": callback_url,
        "metadata": {
            "ignore_hash": data_dict.get("ignore_hash", False),
            "ckan_url": site_url,
            "resource_id": res_id,
            "set_url_type": data_dict.get("set_url_type", False),
            "task_created": task["last_updated"],
            "original_url": resource_dict.get("url"),
        },
    }
    # The flow envelope timeout. ``ckan.datapusher.timeout`` is honored for
    # backward compatibility; new deployments should use
    # ``ckanext.datapusher_plus.flow_timeout`` directly.
    dp_timeout = int(
        tk.config.get(
            "ckanext.datapusher_plus.flow_timeout",
            tk.config.get("ckan.datapusher.timeout", 7200),
        )
    )

    # The DP+ job_id (== task_id) is generated here, before submission, so
    # CKAN's task_status row references the same id the flow will write
    # into the Jobs table via ``add_pending_job``.
    import uuid

    job_id = str(uuid.uuid4())
    job_input = JobInput(
        task_id=job_id,
        resource_id=res_id,
        ckan_url=site_url,
        input=data,
        dry_run=False,
    )
    try:
        run_id, via_prefect = _submit_job(job_input, dp_timeout)
    except Exception as e:
        log.error("Error submitting job to DataPusher: %s", e)
        return False

    # Public contract: ``job_id`` keeps working for v2 consumers (CKAN UI's
    # status page reads it). ``flow_run_id`` is the new field for clients
    # that want to deep-link into the Prefect UI — recorded only when the
    # run really is a Prefect flow run. With Prefect disabled the RQ job
    # id goes in ``rq_job_id`` instead, so nothing tries to build a
    # Prefect URL out of it.
    run_id_key = "flow_run_id" if via_prefect else "rq_job_id"
    value = json.dumps({"job_id": job_id, run_id_key: run_id})
    task["value"] = value
    task["state"] = "pending"
    task["last_updated"] = utcnow_naive().isoformat()
    p.toolkit.get_action("task_status_update")(context, task)

    return True


def datapusher_hook(context: Context, data_dict: dict[str, Any]):
    """Update datapusher task. This action is typically called by the
    datapusher whenever the status of a job changes.

    :param metadata: metadata produced by datapuser service must have
       resource_id property.
    :type metadata: dict
    :param status: status of the job from the datapusher service
    :type status: string
    """

    metadata, status = _get_or_bust(data_dict, ["metadata", "status"])

    res_id = _get_or_bust(metadata, "resource_id")

    # Pass metadata, not data_dict, as it contains the resource id needed
    # on the auth checks. Use the dedicated datapusher_hook permission rather
    # than datapusher_submit so the boundary matches the hook's side effects
    # (marking complete, creating default views, recursive resubmit) and can
    # be tightened independently.
    p.toolkit.check_access("datapusher_hook", context, metadata)

    task = p.toolkit.get_action("task_status_show")(
        context,
        {"entity_id": res_id, "task_type": "datapusher_plus", "key": "datapusher_plus"},
    )

    task["state"] = status
    task["last_updated"] = utcnow_naive().isoformat()

    resubmit = False
    if status == "complete":
        # Create default views for resource if necessary (only the ones that
        # require data to be in the DataStore)
        resource_dict = p.toolkit.get_action("resource_show")(context, {"id": res_id})

        dataset_dict = p.toolkit.get_action("package_show")(
            context, {"id": resource_dict["package_id"]}
        )

        for plugin in p.PluginImplementations(interfaces.IDataPusher):
            plugin.after_upload(
                cast("dict[str, Any]", context), resource_dict, dataset_dict
            )

        try:
            logic.get_action("resource_create_default_resource_views")(
                context,
                {
                    "resource": resource_dict,
                    "package": dataset_dict,
                    "create_datastore_views": True,
                },
            )
        except Exception as e:
            log.error("Error creating default views for resource %s: %s", res_id, e)

        # Check if the uploaded file has been modified in the meantime
        if resource_dict.get("last_modified") and metadata.get("task_created"):
            try:
                last_modified_datetime = parse_date(resource_dict["last_modified"])
                task_created_datetime = parse_date(metadata["task_created"])
                if last_modified_datetime > task_created_datetime:
                    log.debug(
                        "Uploaded file more recent: {0} > {1}".format(
                            last_modified_datetime, task_created_datetime
                        )
                    )
                    resubmit = True
            except ValueError:
                pass
        # Check if the URL of the file has been modified in the meantime
        elif (
            resource_dict.get("url")
            and metadata.get("original_url")
            and resource_dict["url"] != metadata["original_url"]
        ):
            log.debug(
                "URLs are different: {0} != {1}".format(
                    resource_dict["url"], metadata["original_url"]
                )
            )
            resubmit = True

    context["ignore_auth"] = True
    p.toolkit.get_action("task_status_update")(context, task)

    if resubmit:
        log.debug(
            "Resource {0} has been modified, "
            "resubmitting to DataPusher".format(res_id)
        )
        p.toolkit.get_action("datapusher_submit")(context, {"resource_id": res_id})


@side_effect_free
def datapusher_status(context: Context, data_dict: dict[str, Any]) -> dict[str, Any]:
    """Get the status of a datapusher job for a certain resource.

    :param resource_id: The resource id of the resource that you want the
        datapusher status for.
    :type resource_id: string
    """

    p.toolkit.check_access("datapusher_status", context, data_dict)

    if "id" in data_dict:
        data_dict["resource_id"] = data_dict["id"]
    res_id = _get_or_bust(data_dict, "resource_id")

    task = p.toolkit.get_action("task_status_show")(
        context,
        {"entity_id": res_id, "task_type": "datapusher_plus", "key": "datapusher_plus"},
    )

    value = json.loads(task["value"])
    job_key = value.get("job_key")
    job_id = value.get("job_id")
    flow_run_id = value.get("flow_run_id")
    # ``job_url`` is now a Prefect-UI deep-link when available, so the CKAN
    # status page can jump straight into the run graph.
    prefect_ui_base = config.get("ckanext.datapusher_plus.prefect_ui_base")
    url = (
        f"{prefect_ui_base.rstrip('/')}/runs/flow-run/{flow_run_id}"
        if (prefect_ui_base and flow_run_id)
        else None
    )
    job_detail = None

    if job_id:
        job_detail = get_job_details(job_id)

        if job_detail and job_detail.get("logs"):
            for log in job_detail["logs"]:
                if "timestamp" in log and isinstance(
                    log["timestamp"], datetime.datetime
                ):
                    log["timestamp"] = log["timestamp"].isoformat()
    try:
        error = json.loads(task["error"])
    except ValueError:
        error = task["error"]

    return {
        "status": task["state"],
        "job_id": job_id,
        "flow_run_id": flow_run_id,
        "job_url": url,
        "last_updated": task["last_updated"],
        "job_key": job_key,
        "task_info": job_detail,
        "error": error,
    }
