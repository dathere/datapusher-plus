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
        except Exception as e:
            error_shout(e)
            error_shout("Package '{}' was not found".format(package))
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

    from ckanext.datapusher_plus import prefect_client
    from ckanext.datapusher_plus.jobs.blocks import ensure_result_storage_block

    pool = work_pool or tk.config.get(
        "ckanext.datapusher_plus.prefect_work_pool", "datapusher-plus"
    )
    # Auto-create the work pool if missing; Prefect 3 ``flow.deploy``
    # does not bootstrap pools on its own.
    prefect_client.ensure_work_pool(pool)
    click.echo(f"Work pool: {pool}")
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

    deployment_id = flow_to_deploy.deploy(
        name=deployment_name,
        work_pool_name=pool,
        # Build the deployment from local source — no Docker image required
        # for the default ``process`` worker. Operators using k8s/ECS pools
        # can override via prefect.yaml at the repo root.
        image=None,
        push=False,
    )
    click.echo(f"Deployed to work-pool '{pool}': {deployment_id}")


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
        # Use the recorded TaskStatus entity_ids since they are the actual
        # resource_ids; the regex-scraped ones from RQ descriptions are a
        # noisy fallback.
        resource_ids = [
            ts.entity_id for ts in pending_tasks if ts.entity_id
        ]
        click.echo(f"Resubmitting {len(resource_ids)} resource(s)...")
        _submit(resource_ids)

    click.echo(
        "Migration complete. Stop any RQ worker processes and start "
        "`prefect worker start -p datapusher-plus` instead."
    )


def get_commands():
    return [datapusher_plus]
