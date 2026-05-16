# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

import logging
from typing import cast

import click

import ckan.model as model
import ckan.plugins.toolkit as tk
import ckanext.datastore.backend as datastore_backend
from ckan.cli import error_shout

if tk.check_ckan_version("2.10"):
    from ckan.types import Context

log = logging.getLogger(__name__)

question = (
    "Data in any datastore resource that isn't in their source files "
    "(e.g. data added using the datastore API) will be permanently "
    "lost. Are you sure you want to proceed?"
)
requires_confirmation = click.option(
    "--yes", "-y", is_flag=True, help="Always answer yes to questions"
)


def confirm(yes: bool):
    if yes:
        return
    click.confirm(question, abort=True)


@click.group(name="datapusher_plus", short_help="Datapusher Plus commands")
def datapusher_plus():
    """
    Datapusher Plus commands

    Explicit ``name="datapusher_plus"`` to keep the underscore form the
    README documents — Click 8 would otherwise auto-convert the function
    name to ``datapusher-plus``.
    """


# @datapusher_plus.command()
# def init_db():
#     """Initialise the Datapusher Plus tables."""
#     init_tables()
#     print("Datapusher Plus tables created")


@datapusher_plus.command()
@requires_confirmation
def resubmit(yes: bool):
    """Resubmit updated datastore resources."""
    confirm(yes)

    resource_ids = datastore_backend.get_all_resources_ids_in_datastore()
    _submit(resource_ids)


@datapusher_plus.command()
@click.argument("package", required=False)
@requires_confirmation
def submit(package: str, yes: bool):
    """Submits resources from package.

    If no package ID/name specified, submits all resources from all
    packages.
    """
    confirm(yes)

    if not package:
        ids = tk.get_action("package_list")(
            cast(Context, {"model": model, "ignore_auth": True}), {}
        )
    else:
        ids = [package]

    for id in ids:
        package_show = tk.get_action("package_show")
        try:
            pkg = package_show(
                cast(Context, {"model": model, "ignore_auth": True}), {"id": id}
            )
        except tk.ObjectNotFound:
            # The original code said "was not found" but caught *all* exceptions
            # and logged the wrong identifier (`package`, not the current `id`).
            error_shout("Package '{}' was not found".format(id))
            raise click.Abort()
        except tk.NotAuthorized:
            error_shout("Not authorized to read package '{}'".format(id))
            raise click.Abort()
        except Exception as e:
            error_shout("Unexpected error reading package '{}': {}".format(id, e))
            raise click.Abort()
        if not pkg["resources"]:
            continue
        resource_ids = [r["id"] for r in pkg["resources"]]
        _submit(resource_ids)


def _submit(resources: list[str]):
    click.echo("Submitting {} datastore resources".format(len(resources)))
    user = tk.get_action("get_site_user")(
        cast(Context, {"model": model, "ignore_auth": True}), {}
    )
    datapusher_submit = tk.get_action("datapusher_submit")
    for id in resources:
        click.echo("Submitting {}...".format(id), nl=False)
        data_dict = {
            "resource_id": id,
            "ignore_hash": True,
        }
        if datapusher_submit({"user": user["name"]}, data_dict):
            click.echo("OK")
        else:
            click.echo("Fail")


@datapusher_plus.command(name="prefect-deploy")
@click.option(
    "--work-pool",
    "work_pool",
    default=None,
    help="Override the work-pool name (defaults to ckanext.datapusher_plus.prefect_work_pool).",
)
def prefect_deploy(work_pool: str | None):
    """Register the DataPusher+ flow with the configured Prefect server.

    Idempotent: re-running updates the existing deployment in place. Also
    registers the default result-storage Block so "re-run from failed task"
    in the Prefect UI works out of the box.

    The flow being deployed comes from
    ``ckanext.datapusher_plus.prefect_flow`` when set (an importable
    ``module.path:flow_name`` entrypoint) and falls back to the built-in
    ``datapusher_plus_flow`` otherwise. This is how operators register
    custom ingestion flows without modifying DP+.
    """
    try:
        from prefect import flow as _flow_decorator  # noqa: F401
        from prefect.deployments.runner import RunnerDeployment  # noqa: F401
    except Exception as e:
        error_shout(f"Prefect is not installed: {e}")
        raise click.Abort()

    from pathlib import Path

    import ckanext.datapusher_plus as dp_pkg
    from ckanext.datapusher_plus import prefect_client
    from ckanext.datapusher_plus.jobs.blocks import ensure_result_storage_block

    pool = work_pool or tk.config.get(
        "ckanext.datapusher_plus.prefect_work_pool", "datapusher-plus"
    )
    pool_type = tk.config.get(
        "ckanext.datapusher_plus.prefect_work_pool_type", "process"
    )
    # Auto-create the work pool if missing; Prefect 3 ``flow.deploy``
    # does not bootstrap pools on its own. The type defaults to
    # ``process`` — operators on k8s/Docker/ECS set
    # ``ckanext.datapusher_plus.prefect_work_pool_type`` so the pool is
    # created with the right topology rather than a silent ``process``.
    prefect_client.ensure_work_pool(pool, pool_type=pool_type)
    click.echo(f"Work pool: {pool} (type: {pool_type})")
    block_id = ensure_result_storage_block()
    click.echo(f"Result-storage block: {block_id}")

    try:
        flow_to_deploy = prefect_client.resolve_flow()
    except ImportError as e:
        error_shout(str(e))
        raise click.Abort()

    custom = tk.config.get("ckanext.datapusher_plus.prefect_flow", "")
    if custom:
        click.echo(f"Deploying custom flow: {custom}")
    else:
        click.echo("Deploying built-in flow: datapusher_plus_flow")

    # The deployment-name component is the second half of
    # ``ckanext.datapusher_plus.prefect_deployment_name`` (``<flow>/<name>``).
    deployment_name = tk.config.get(
        "ckanext.datapusher_plus.prefect_deployment_name",
        "datapusher-plus/datapusher-plus",
    ).split("/", 1)[-1]

    if not custom:
        # Prefect 3 ``flow.deploy()`` requires either an image OR a
        # storage location, even for local ``process`` workers. Point at
        # the locally-installed DP+ source via ``from_source(...)`` so
        # the worker pulls the flow from there at run time. For a custom
        # flow, the operator is on the hook for arranging their own
        # storage / image (typically via prefect.yaml).
        # Anchor the deployment source at the installed DP+ package
        # directory itself, not three levels up. Walking ``.parent`` x3
        # happens to land on the repo root for an editable install but
        # lands in site-packages — or the wrong namespace-package root
        # when other ``ckanext.*`` packages are installed alongside —
        # for a regular install, leaving the entrypoint unresolvable at
        # run time. ``dp_pkg.__file__`` is always
        # ``<root>/ckanext/datapusher_plus/__init__.py``; its parent is
        # the package dir, regardless of install layout.
        dp_pkg_dir = Path(dp_pkg.__file__).resolve().parent
        click.echo(f"Deploying flow from local source: {dp_pkg_dir}")
        flow_to_deploy = flow_to_deploy.from_source(
            source=str(dp_pkg_dir),
            entrypoint="jobs/prefect_flow.py:datapusher_plus_flow",
        )

    deployment_id = flow_to_deploy.deploy(
        name=deployment_name,
        work_pool_name=pool,
        # ``image=None`` and ``push=False`` because the default ``process``
        # worker reads source from the from_source path above, not from a
        # Docker registry. Operators on k8s/ECS push pools provide a
        # custom flow + their own prefect.yaml.
        image=None,
        push=False,
        # Sentinel read by ``prefect_flow._bootstrap_ckan_app_context`` so
        # it can tell a real worker subprocess apart from pytest / ad-hoc
        # imports / tooling, and only call ``make_app()`` for the former.
        # Prefect merges these vars into the worker subprocess's env at
        # flow-run start; ``CKAN_INI`` continues to come from the worker's
        # own environment (operator-set per deployment).
        job_variables={"env": {"DPP_PREFECT_WORKER": "1"}},
    )
    click.echo(f"Deployed to work-pool '{pool}': {deployment_id}")
    # Surface the sentinel-env-var contract for operators. ``prefect-deploy``
    # injects it for them, but anyone who later moves to a hand-rolled
    # ``prefect.yaml`` needs to replicate it -- without the sentinel the
    # worker's import-time bootstrap no-ops and the first ``tk.get_action``
    # call later crashes with a stale-action-registry error.
    click.echo(
        "  Worker subprocess env will receive DPP_PREFECT_WORKER=1 (sentinel "
        "for _bootstrap_ckan_app_context). If you deploy via your own "
        "prefect.yaml instead of this command, replicate it under "
        "job_variables.env -- the bootstrap is a no-op without it."
    )


@datapusher_plus.command(name="migrate-from-rq")
@click.option(
    "--resubmit/--no-resubmit",
    default=False,
    help="After draining RQ, resubmit the recorded resource_ids through Prefect.",
)
@requires_confirmation
def migrate_from_rq(resubmit: bool, yes: bool):
    """One-shot v2 → v3 migration.

    Drains the RQ queue, resets any ``pending`` task_status rows so the
    CKAN UI no longer shows them as in-flight, verifies the Prefect
    deployment exists, and optionally resubmits the recorded resources
    through the new Prefect path.
    """
    confirm(yes)

    drained: list[str] = []
    # Try to drain the RQ queue, if RQ is still installed. New installs
    # will skip this branch cleanly.
    try:
        import ckan.lib.jobs as rq_jobs

        queue = rq_jobs.get_queue()
        for job in list(queue.get_jobs()):
            if "push_to_datastore" in (job.description or "") or \
               "datapusher" in (job.description or ""):
                # Best-effort regex on the legacy ``[{'resource_id': '...'}]``
                # description format.
                import re

                m = re.search(r"'resource_id':\s*u?'([^']+)'", job.description or "")
                if m:
                    drained.append(m.group(1))
                job.cancel()
        click.echo(f"Drained {len(drained)} RQ jobs from queue.")
    except Exception as e:
        click.echo(f"RQ queue not reachable (assuming already removed): {e}")

    # Reset any ``pending`` task_status rows so the UI does not falsely
    # show in-flight ingestions that no longer have a worker.
    import datetime
    import json

    from ckan import model as ckan_model

    session = ckan_model.Session
    reset_count = 0
    # Snapshot the resource_ids to resubmit *before* committing. With the
    # default ``expire_on_commit=True``, reading ``ts.entity_id`` after the
    # commit would re-SELECT each row (N round-trips) and raise
    # ObjectDeletedError on any concurrently-deleted row.
    resubmit_resource_ids: list[str] = []
    pending_tasks = (
        session.query(ckan_model.TaskStatus)
        .filter_by(task_type="datapusher_plus", state="pending")
        .all()
    )
    for ts in pending_tasks:
        try:
            value = json.loads(ts.value) if ts.value else {}
        except Exception:
            value = {}
        if "flow_run_id" in value:
            continue  # Already on Prefect path.
        # Snapshot only the tasks being reset here. Tasks already on the
        # Prefect path (continue'd above) are intentionally excluded so
        # --resubmit does not double-submit a flow that is already
        # running.
        if ts.entity_id:
            resubmit_resource_ids.append(ts.entity_id)
        ts.state = "error"
        ts.error = json.dumps({"message": "migrated to Prefect; please resubmit"})
        ts.last_updated = datetime.datetime.utcnow()
        reset_count += 1
        if value.get("job_id"):
            drained.append(value["job_id"])  # Not a resource_id but harmless
    session.commit()
    click.echo(f"Reset {reset_count} stale ``pending`` task_status rows.")

    # Sanity-check Prefect server reachability.
    try:
        import ckanext.datapusher_plus.prefect_client as prefect_client

        prefect_client.get_running_resource_ids()
        click.echo("Prefect server is reachable.")
    except Exception as e:
        error_shout(f"Cannot reach Prefect server: {e}")
        raise click.Abort()

    # Optional: resubmit each drained resource through the new path.
    if resubmit:
        # Use the pre-commit snapshot of TaskStatus entity_ids — they are
        # the actual resource_ids; the regex-scraped ones from RQ
        # descriptions are a noisy fallback.
        click.echo(f"Resubmitting {len(resubmit_resource_ids)} resource(s)...")
        _submit(resubmit_resource_ids)

    click.echo(
        "Migration complete. Stop any RQ worker processes and start "
        "`prefect worker start -p datapusher-plus` instead."
    )


def get_commands():
    return [datapusher_plus]
