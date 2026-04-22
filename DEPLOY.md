# SageRx deploy (rxreport fork)

This file only exists on the `deploy` branch. Upstream `main` stays unmodified so
rebases against `coderxio/sagerx` never conflict with our deploy config.

## What runs where

- **Host**: `app.rxreport.com` (Linode, `rxreport` user, SSH key on file)
- **Install path**: `/opt/rxreport/rxreport-app/sagerx/`
- **UI**: <https://sagerx.rxreport.space> (nginx → `127.0.0.1:8001` → Airflow webserver; Let's Encrypt)
- **Warehouse**: the bundled Postgres 14 container (`postgres:5432`, DB `sagerx`, creds
  `sagerx:sagerx` — upstream defaults). Lives on the same container that holds
  Airflow metadata; different database name.
- **NOT on Neon**: the `rxreport` app's `neondb` is untouched by sagerx. Managed
  Neon rejects server-side `COPY <table> FROM '<path>'` (the pattern in sagerx's
  ~40 load SQL files) because `pg_read_server_files` is superuser-only there.
  That blocked the "warehouse on Neon" plan — we run the warehouse locally instead.

## Host directory layout

```
/opt/rxreport/rxreport-app/sagerx/
├── repo/                               # git clone of rxreport/sagerx
│                                       # tracks origin/deploy — NEVER edited in place
├── overrides/
│   ├── docker-compose.override.yml     # joins rxreport_net, !override ports,
│   │                                   # group_add:[988] for docker-socket DAGs
│   ├── profiles.yml                    # dbt → local bundled postgres
│   └── .env                            # ALL secrets, 600 perms
├── data/airflow-pg/                    # Airflow metadata + sagerx warehouse bind-mount
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

## First-boot Postgres state

When `data/airflow-pg/` is empty, the bundled `postgres:14-alpine` container runs
`repo/postgres/*.sql` init scripts on first boot. These upstream scripts
automatically create:

- `airflow` database + `airflow` user (Airflow metadata)
- `sagerx_lake`, `sagerx_dev`, `sagerx` schemas in the `sagerx` database
- `sagerx.data_availability` table
- `sagerx.product_agg` aggregate + `_final_product` function

No manual SQL needed for a fresh install. If you ever need to reset the warehouse,
`rm -rf data/airflow-pg/*` and re-run `deploy-sagerx.sh` — the init scripts will
re-seed everything (but you'll lose Airflow run history too, so don't unless you
mean to).

## Important override details

- **`group_add: ["988"]`** on all airflow services — the host docker group gid.
  Airflow 2.5.1's entrypoint requires `gid=0` as the primary group, so we add 988
  as a supplementary group. This gives DAGs that shell out via `docker exec dbt ...`
  (`mccpd.transform`, `build_marts`, `export_marts`, etc.) access to
  `/var/run/docker.sock`. If this gid changes on the host, update `overrides/docker-compose.override.yml`.

- **`AIRFLOW_CONN_POSTGRES_DEFAULT`** and **`AIRFLOW_CONN_SAGERX_WAREHOUSE`** both
  point at `postgresql://sagerx:sagerx@postgres:5432/sagerx`. Airflow auto-registers
  these as connections on webserver/scheduler boot.

- **`ports: !override`** (Compose v2.24+ tag) on `postgres`, `airflow-webserver`,
  and `dbt`. Without it, compose MERGES port lists, so the base's public
  `0.0.0.0:5432` / `0.0.0.0:8081` bindings would stick around. With it, we get
  only `127.0.0.1:8001` (Airflow UI, behind nginx) exposed on the host.

- **pgadmin and marimo services are skipped** (not listed in the `up` command).

- **`shm_size: 2g`** on the `postgres` service (added 2026-04-22). The Docker default of 64MB is far too small for parallel-query workers. Without this, large dbt intermediate models (`int_rxnorm_clinical_products_to_ingredient_components` was the canary) fail mid-transform with `could not resize shared memory segment "/PostgreSQL.*": No space left on device` — that "disk" is `/dev/shm`, not the host volume. 2GB covers every model we've seen; raise if new giant models appear.

## Host sizing

- **Volume**: the `/opt` attached volume holds **everything** — `sagerx/`, rxreport backend+frontend, MSSQL data, Docker layers. Currently **300GB** (resized up from 99GB on 2026-04-22 after a disk-full outage took postgres down).
- **Main consumers**: `sagerx/repo/airflow/data/` (per-DAG download cache — `cms_part_d` ~25GB, `dailymed` ~20GB, `rxnorm` ~2GB per fresh run) + Docker images (~25GB) + warehouse `data/airflow-pg/` (grows with ingestion — target ~50GB when everything is loaded).
- **Failed DAGs leak disk**: ingestion DAGs download their source zip, extract, and only clean up on success. Failed runs leave the extracted files behind, and retries add another copy. Watch `sagerx/repo/airflow/data/<dag_id>/` during incidents — safe to `rm -rf` any dir whose DAG's last run is failed, the data re-downloads on retry.

## Rebasing on upstream

Upstream lives at <https://github.com/coderxio/sagerx>. Pulling updates:

```bash
git remote add upstream https://github.com/coderxio/sagerx.git
git fetch upstream
git checkout main && git merge --ff-only upstream/main
git push origin main

# Then fast-forward deploy to main + the deploy-only commits
git checkout deploy
git rebase main         # should be clean: DEPLOY.md + .github/workflows/linode-deploy.yml only
git push origin deploy  # triggers GHA and redeploys
```

## Future migration path: warehouse on Neon

If there's ever a reason to move the warehouse back to Neon (e.g. to get managed
backups / point-in-time recovery), the blocking issue is the ~40 load SQL files
that use server-side `COPY FROM '<path>'`. The fix would be:

1. Add a `CopyFromFileOperator` helper in `airflow/plugins/` that reads the existing
   SQL, rewrites `COPY ... FROM '<path>'` to `COPY ... FROM STDIN`, opens the file
   client-side, and pipes via `psycopg2.copy_expert`.
2. Replace `PostgresOperator(sql=read_sql_file(x))` with the new operator in the
   ~17 affected `dag.py` files. SQL files stay unchanged.
3. Flip `AIRFLOW_CONN_POSTGRES_DEFAULT` / `AIRFLOW_CONN_SAGERX_WAREHOUSE` / dbt
   profile to point at Neon.

The `sagerx_worker` Neon role and empty `sagerx_*` schemas are still present on
`neondb` from the abandoned plan — harmless, drop whenever.
