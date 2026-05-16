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

# Shared between ``resubmit`` and ``submit``. Default ``True`` preserves
# the legacy "drain the list" behaviour (every resource gets a turn,
# even after earlier ones fail). ``--stop-on-error`` is for users
# wiring this into CI who want to bail on the first failure.
continue_on_error_option = click.option(
    "--continue-on-error/--stop-on-error",
    "continue_on_error",
    default=True,
    show_default=True,
    help=(
        "When a resource fails or errors, continue submitting the rest "
        "(default) or stop at the first non-OK outcome."
    ),
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
@continue_on_error_option
def resubmit(yes: bool, continue_on_error: bool):
    """Resubmit updated datastore resources.

    Exits non-zero when at least one resource didn't successfully
    submit, so this can be wired into CI / cron with proper error
    surfacing.
    """
    confirm(yes)

    resource_ids = datastore_backend.get_all_resources_ids_in_datastore()
    if not _submit(resource_ids, continue_on_error=continue_on_error):
        raise click.exceptions.Exit(code=1)


@datapusher_plus.command()
@click.argument("package", required=False)
@requires_confirmation
@continue_on_error_option
def submit(package: str, yes: bool, continue_on_error: bool):
    """Submits resources from package.

    If no package ID/name specified, submits all resources from all
    packages. Exits non-zero when at least one resource didn't
    successfully submit (across all packages).
    """
    confirm(yes)

    if not package:
        ids = tk.get_action("package_list")(
            cast(Context, {"model": model, "ignore_auth": True}), {}
        )
    else:
        ids = [package]

    any_failures = False
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
        if not _submit(resource_ids, continue_on_error=continue_on_error):
            any_failures = True
            if not continue_on_error:
                # stop processing further packages too
                break

    if any_failures:
        raise click.exceptions.Exit(code=1)


def _submit(
    resources: list[str],
    *,
    continue_on_error: bool = True,
) -> bool:
    """Submit a batch of resources to the datapusher-plus pipeline.

    Each resource lands in exactly one of three buckets:

    * ``ok``    — ``datapusher_submit`` returned truthy.
    * ``fail``  — ``datapusher_submit`` returned falsy (declined / a
      precondition such as the resource format gate wasn't met).
    * ``error`` — ``datapusher_submit`` raised. The exception is
      recorded against the resource id but doesn't bring the whole
      batch down (so a single auth/network blip on one resource
      doesn't lose all subsequent submissions).

    Returns ``True`` iff every resource ended up in ``ok``. Callers
    use this to set a non-zero exit status when at least one resource
    didn't successfully submit — the prior implementation always
    exited 0 regardless of how many "Fail" lines it printed, which
    made `resubmit` impossible to wire into CI / monitoring.

    Args:
        resources: Resource ids to submit, in order.
        continue_on_error: When ``False``, the first non-ok outcome
            stops the loop. Default ``True`` (legacy behaviour: drain
            the list).
    """
    total = len(resources)
    click.echo(f"Submitting {total} datastore resource(s)")
    if total == 0:
        return True

    user = tk.get_action("get_site_user")(
        cast(Context, {"model": model, "ignore_auth": True}), {}
    )
    datapusher_submit = tk.get_action("datapusher_submit")

    ok: list[str] = []
    failed: list[str] = []
    errored: list[tuple[str, str]] = []
    stopped_early = False

    for idx, resource_id in enumerate(resources, 1):
        click.echo(f"[{idx}/{total}] Submitting {resource_id}... ", nl=False)
        data_dict = {"resource_id": resource_id, "ignore_hash": True}
        try:
            submitted = datapusher_submit({"user": user["name"]}, data_dict)
        except Exception as exc:
            click.echo(f"ERROR: {exc}")
            log.exception(
                "datapusher_submit raised for resource %s", resource_id
            )
            errored.append((resource_id, str(exc)))
            if not continue_on_error:
                stopped_early = True
                break
            continue

        if submitted:
            click.echo("OK")
            ok.append(resource_id)
        else:
            click.echo("Fail")
            failed.append(resource_id)
            if not continue_on_error:
                stopped_early = True
                break

    _print_submit_summary(
        total, ok, failed, errored, stopped_early=stopped_early
    )
    return not failed and not errored


def _print_submit_summary(
    total: int,
    ok: list[str],
    failed: list[str],
    errored: list[tuple[str, str]],
    *,
    stopped_early: bool,
) -> None:
    """Render the end-of-batch summary block.

    Split out so unit tests can assert on the per-bucket lists
    without driving the whole submit loop.
    """
    attempted = len(ok) + len(failed) + len(errored)
    not_attempted = total - attempted

    click.echo("")
    click.echo("Submit summary:")
    click.echo(f"  OK:      {len(ok)} / {total}")
    click.echo(f"  Fail:    {len(failed)}")
    click.echo(f"  Error:   {len(errored)}")
    if stopped_early and not_attempted:
        click.echo(f"  Skipped: {not_attempted} (stopped on first failure)")

    if failed:
        click.echo("Failed resources (declined / preconditions not met):")
        for rid in failed:
            click.echo(f"  - {rid}")

    if errored:
        click.echo("Errored resources (exception raised):")
        for rid, message in errored:
            click.echo(f"  - {rid}: {message}")


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
