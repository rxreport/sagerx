# SageRx deploy (rxreport fork)

This file only exists on the `deploy` branch. Upstream `main` stays unmodified so
rebases against `coderxio/sagerx` never conflict with our deploy config.

## What runs where

- **Host**: `app.rxreport.com` (Linode, `rxreport` user, SSH key on file)
- **Install path**: `/opt/rxreport/rxreport-app/sagerx/`
- **UI**: <https://sagerx.rxreport.space> (nginx → `127.0.0.1:8001` → Airflow webserver; Let's Encrypt)
- **Warehouse**: shares the `neondb` Neon DB with the rxreport backend — sagerx writes
  only to `sagerx_lake`, `sagerx_dev`, `sagerx` schemas under the `sagerx_worker` role
- **Airflow metadata**: bundled Postgres 14, bind-mount at `overrides/../data/airflow-pg/`

## Host directory layout

```
/opt/rxreport/rxreport-app/sagerx/
├── repo/                               # git clone of rxreport/sagerx
│                                       # tracks origin/deploy — NEVER edited in place
├── overrides/
│   ├── docker-compose.override.yml     # joins rxreport_net, !override ports, env wiring
│   ├── profiles.yml                    # dbt → Neon (env-substituted)
│   └── .env                            # ALL secrets, 600 perms
├── data/airflow-pg/                    # Airflow metadata Postgres bind-mount
└── deploy-sagerx.sh                    # the orchestrator this workflow calls
```

The overrides directory lives OUTSIDE the repo clone so that `git pull` stays
fast-forward-only. Never commit overrides back to the repo.

## Deploy flow

Push to `deploy` branch → `.github/workflows/linode-deploy.yml` fires →
appleboy/ssh-action SSHes into the Linode → runs
`/opt/rxreport/rxreport-app/sagerx/deploy-sagerx.sh`, which:

1. `git fetch origin && git checkout deploy && git pull --ff-only origin deploy`
2. Updates `UMLS_API` in `overrides/.env` from the `UMLS_KEY` org secret passed in
3. Symlinks `repo/.env -> ../overrides/.env` so the base compose's `env_file: .env`
   directive resolves to our secrets file
4. Ensures `repo/gcp.json` exists as a FILE (not a directory — Docker silently
   creates the latter when a file bind-mount's source is missing)
5. `docker compose build airflow-webserver dbt` (retries 3× on transient docker.io
   auth errors — the base image pulls sometimes fail)
6. `docker compose pull postgres`
7. `docker compose up -d postgres` → waits for `pg_isready`
8. `docker compose up --no-deps airflow-init` (blocks until initialized)
9. `docker compose up -d --remove-orphans airflow-webserver airflow-scheduler dbt`

Manual invocation:

```bash
ssh rxreport@app.rxreport.com '/opt/rxreport/rxreport-app/sagerx/deploy-sagerx.sh'
```

## GitHub Actions secrets

All four are **org-level** secrets on `rxreport`. No repo-level secrets.

| Secret | Purpose |
| --- | --- |
| `LINODE_HOST` | SSH target (`app.rxreport.com`) |
| `LINODE_USER` | SSH user (`rxreport`) |
| `LINODE_SSH_KEY` | ed25519 private key |
| `UMLS_KEY` | RxNorm / UMLS API key, exported to the host as `UMLS_API` |

If the workflow fails immediately on SSH, confirm `rxreport/sagerx` is in the
"selected repositories" list on each of the four org secrets at
Org → Settings → Secrets → Actions.

## Neon bootstrap (one-time, already done)

Against the `neondb_owner` DSN — only if re-installing from scratch:

```sql
CREATE SCHEMA IF NOT EXISTS sagerx_lake;
CREATE SCHEMA IF NOT EXISTS sagerx_dev;
CREATE SCHEMA IF NOT EXISTS sagerx;

CREATE ROLE sagerx_worker LOGIN PASSWORD '...';
GRANT sagerx_worker TO CURRENT_USER;           -- Neon requires this before OWNER TO
REVOKE ALL ON SCHEMA public FROM sagerx_worker;

ALTER SCHEMA sagerx_lake OWNER TO sagerx_worker;
ALTER SCHEMA sagerx_dev  OWNER TO sagerx_worker;
ALTER SCHEMA sagerx      OWNER TO sagerx_worker;

SET ROLE sagerx_worker;

CREATE TABLE sagerx.data_availability (
    schema_name text, table_name text, has_data boolean, materialized text
);

CREATE OR REPLACE FUNCTION sagerx._final_product(numeric) RETURNS numeric
  LANGUAGE sql IMMUTABLE STRICT AS 'SELECT $1';

CREATE AGGREGATE sagerx.product_agg(numeric) (
  SFUNC = numeric_mul, STYPE = numeric, INITCOND = '1',
  FINALFUNC = sagerx._final_product
);

RESET ROLE;
```

> `data_availability` lives in the `sagerx` mart schema per upstream
> `postgres/2_sagerx_setup.sql` — not in `sagerx_lake` despite the name.

## Known deviations from upstream

- `product_agg` aggregate installed in `sagerx` schema, not `public`. Any DAG or
  dbt model that calls `product_agg(...)` unqualified will fail — qualify as
  `sagerx.product_agg(...)`.
- `AIRFLOW_CONN_POSTGRES_DEFAULT` redirected to Neon (`sagerx_worker@neondb`)
  instead of the bundled `sagerx:sagerx@postgres:5432/sagerx`. DAGs that hardcode
  the bundled DSN will not work against Neon. Fix on a case-by-case basis.
- `pgadmin` and `marimo` services are skipped (not included in the up command).
- All host ports except `127.0.0.1:8001` (Airflow UI, behind nginx) are unbound.

## Rebasing on upstream

Upstream lives at <https://github.com/coderxio/sagerx>. Pulling updates:

```bash
git fetch upstream
git checkout main && git merge --ff-only upstream/main
git push origin main

# Then fast-forward deploy to main + the workflow commit
git checkout deploy
git rebase main         # should be clean: DEPLOY.md and linode-deploy.yml only
git push origin deploy  # triggers GHA and redeploys
```
