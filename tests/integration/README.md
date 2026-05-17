# DataPusher+ integration tests

These tests exercise the full v3.0 ingestion path against a live stack
of CKAN, Prefect, Postgres, Solr, and Redis. They are gated behind the
`INTEGRATION=1` environment variable so a normal `pytest tests/` run
skips them.

## What's covered

| Test | Scenario |
|---|---|
| `test_csv_resource_lands_in_datastore` | Upload a CSV → flow completes → rows queryable from the datastore → flow run visible in Prefect API |
| `test_resubmit_same_resource_hits_download_cache` | Two submits of the same unchanged resource → the second run's download task ends in `Cached` state |

Add more scenarios (quarantine, suspend-on-PII, transactional rollback,
worker crash) here as you exercise them locally.

## Stack layout

`docker-compose.integration.yaml` brings up:

```
postgres (shared)   Solr   Redis
       │              │     │
       │              │     │
   ┌───┴──────────────┴─────┴──┐
   │           CKAN            │  http://localhost:5000
   └──────────┬────────────────┘
              │
              │ submit_flow_run
              ▼
   ┌──────────────────────────┐
   │     Prefect server       │  http://localhost:4200
   └──────────┬───────────────┘
              │ poll
              ▼
   ┌──────────────────────────┐
   │     Prefect worker       │  (qsv-enabled, repo bind-mounted)
   └──────────────────────────┘
```

The worker image is built from `Dockerfile.worker`. It extends
`ckan/ckan-dev:2.11` (the same base as the CKAN service — DP+'s
flow code needs CKAN in the worker's Python env for the
`_bootstrap_ckan_app_context()` make_app call) with GDAL/spatial
system libs, MUSL `qsv` build, and `uchardet`/`file` for
ValidationStage.

## Developer workflow (recommended)

For active development — leaving the stack running between sessions so
you don't pay the full bootstrap cost on every iteration — use the
helpers:

```bash
# Bring it up (idempotent; safe to re-run). Auto-detects cold start
# vs. post-restart vs. post-down-with-volume-kept and does the right
# thing. ~5–8 min cold, ~30 s warm.
scripts/integration-up

# After ``up``, the admin token is in ./.integration-token (gitignored).
# Run the integration tests:
INTEGRATION=1 \
  CKAN_URL=http://localhost:5050 \
  CKAN_API_KEY=$(cat .integration-token) \
  pytest tests/integration/ -v

# Done for now? Two choices:
scripts/integration-down            # keep postgres volume (warm restart later)
scripts/integration-down --wipe     # nuke everything (cold start next time)

# Edited Dockerfile.worker?
scripts/integration-up --rebuild    # force --no-cache rebuild of the worker image
```

Both scripts honour the same port-override env vars listed below, so
``CKAN_HOST_PORT=5050 POSTGRES_HOST_PORT=5433`` (the defaults) keep
the stack out of the way of macOS AirPlay (port 5000) and any other
local Postgres (port 5432).

## Manual run procedure (single-shot CI-style)

If you'd rather drive ``docker compose`` directly — say, for a
one-shot CI repro — the bootstrap inside the ``ckan`` service does
the heavy lifting for you (creates admin user, mints token, injects
it via ``config-tool``). All you have to do is wait, grab the token,
and register the deployment:

```bash
# 1. Bring up the stack (first run also builds the worker image)
CKAN_HOST_PORT=5050 POSTGRES_HOST_PORT=5433 \
    docker compose -f docker-compose.integration.yaml up -d --build

# 2. Wait for CKAN to finish its first-start bootstrap
until curl -fsS http://localhost:5050/api/3/action/status_show >/dev/null 2>&1; do
    sleep 5
done

# 3. Grab the admin token (auto-minted by the bootstrap)
TOKEN=$(docker exec datapusher-plus-ckan-1 cat /tmp/integration_admin_token)

# 4. Register the DP+ deployment with the Prefect server
docker exec datapusher-plus-ckan-1 \
    ckan -c /etc/ckan/default/ckan.ini datapusher_plus prefect-deploy

# 5. Run the integration tests
INTEGRATION=1 CKAN_URL=http://localhost:5050 CKAN_API_KEY=$TOKEN \
    pytest tests/integration/ -v

# 6. (Optional) Watch the runs in the Prefect UI
open http://localhost:4200

# 7. Tear down
docker compose -f docker-compose.integration.yaml down -v
```

## Environment variables

### Test-runner side

| Var | Default | Purpose |
|---|---|---|
| `INTEGRATION` | _(unset)_ | Set to `1` to opt into integration tests. |
| `CKAN_URL` | `http://localhost:5000` | Where CKAN listens (set to `http://localhost:5050` if you used the helper-script default). |
| `PREFECT_URL` | `http://localhost:4200` | Where the Prefect API listens. |
| `CKAN_API_KEY` | _(required)_ | Token for a sysadmin user. After `scripts/integration-up`, this is `$(cat .integration-token)`. |

### Stack-side (override the published host ports)

`docker-compose.integration.yaml` reads these at compose-up time. Pass
them as env vars before `docker compose` (or `scripts/integration-up`)
to dodge port conflicts. macOS users almost always need at least the
first two — `:5000` is AirPlay Receiver and `:5432` is commonly held
by another local Postgres.

| Var | Default | Use when |
|---|---|---|
| `CKAN_HOST_PORT` | `5000` | macOS AirPlay holds `:5000` (`scripts/integration-up` defaults to `5050`). |
| `POSTGRES_HOST_PORT` | `5432` | Another Postgres is already bound (`scripts/integration-up` defaults to `5433`). |
| `PREFECT_HOST_PORT` | `4200` | Conflict with another Prefect server. |
| `SOLR_HOST_PORT` | `8983` | Conflict with another Solr. |
| `REDIS_HOST_PORT` | `6379` | Conflict with another Redis. |

## Troubleshooting

- **`postgres` keeps restarting**: the init script in
  `tests/integration/init-postgres.sh` only runs on a fresh volume.
  Run `docker compose down -v` to wipe it, then re-`up`.
- **Worker is "Late"**: confirm the worker image built with qsv
  (`docker compose logs prefect-worker | head`) and that
  `PREFECT_API_URL` is reachable from within the worker container.
- **`datapusher_submit` returns `False`**: the most common cause is a
  missing CKAN ↔ Prefect-server route. Check
  `docker compose logs ckan | tail` for connection errors.
- **Flow runs forever in `Pending`**: nothing is polling the work pool.
  Make sure `prefect-deploy` ran inside the CKAN container and that
  `prefect-worker` is up.
