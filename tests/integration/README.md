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

The worker image is built from `Dockerfile.worker` (extends
`prefecthq/prefect:3-latest` with qsv).

## Run procedure

```bash
# 1. Bring up the stack (first time also builds the worker image)
docker compose -f docker-compose.integration.yaml up -d --build

# 2. Wait for services to be healthy
docker compose -f docker-compose.integration.yaml ps

# 3. Create the sysadmin user and an API token
docker compose -f docker-compose.integration.yaml exec ckan \
    ckan -c /etc/ckan/default/ckan.ini sysadmin add admin password=admin email=admin@test.local
TOKEN=$(docker compose -f docker-compose.integration.yaml exec ckan \
    ckan -c /etc/ckan/default/ckan.ini user token add admin integration \
    | awk '/api_token/ {print $NF}')

# 4. Register the DP+ deployment with the Prefect server
docker compose -f docker-compose.integration.yaml exec ckan \
    ckan -c /etc/ckan/default/ckan.ini datapusher_plus prefect-deploy

# 5. Run the integration tests
INTEGRATION=1 CKAN_API_KEY=$TOKEN pytest tests/integration/ -v

# 6. (Optional) Watch the runs in the Prefect UI
open http://localhost:4200

# 7. Tear down
docker compose -f docker-compose.integration.yaml down -v
```

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `INTEGRATION` | _(unset)_ | Set to `1` to opt into integration tests. |
| `CKAN_URL` | `http://localhost:5000` | Where CKAN listens. |
| `PREFECT_URL` | `http://localhost:4200` | Where the Prefect API listens. |
| `CKAN_API_KEY` | _(required)_ | Token for a sysadmin user. |

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
