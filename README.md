# SQL Monitor

This app has two runtime modes in one image:
- `collector`: polls SQL Server and writes data into Postgres
- `web`: serves a dashboard/API from Postgres

## Prerequisites
- Docker + Docker Compose
- Reachable SQL Server instance

## Run With Docker Compose (Full Stack)
This starts Postgres + collector + web dashboard.

1. Create compose env file:
   - `cp .env.compose.example .env`
2. Edit `.env` and set at minimum:
   - `SQLSERVER_CONN_STR`
3. Start stack:
   - `docker compose up --build -d`
4. Open dashboard:
   - `http://localhost:8000`
   - If you set `WEB_BIND_PORT`, use that port instead.

Useful commands:
- View logs: `docker compose logs -f web collector`
- Stop stack: `docker compose down`
- Stop and remove Postgres data volume: `docker compose down -v`

## Run Dockerfile Directly (Single Container)
Build image:
- `docker build -t sql-monitor:latest .`

Run collector:
- `docker run --rm \
  -e APP_MODE=collector \
  -e SQLSERVER_CONN_STR='mssql+pymssql://user:pass@host:1433/db' \
  -e POSTGRES_CONN_STR='host=<pg-host> port=5432 dbname=sql_monitor user=sql_monitor password=sql_monitor' \
  sql-monitor:latest`

Run web:
- `docker run --rm -p 8000:8000 \
  -e APP_MODE=web \
  -e POSTGRES_CONN_STR='host=<pg-host> port=5432 dbname=sql_monitor user=sql_monitor password=sql_monitor' \
  sql-monitor:latest`

## Notes
- If SQL Server runs on your Mac/Windows host, use `host.docker.internal` inside `SQLSERVER_CONN_STR`.
- Dashboard auth:
  - Set `DASHBOARD_AUTH_USERNAME` and `DASHBOARD_AUTH_PASSWORD` to enable auth automatically.
  - Or set `DASHBOARD_AUTH_ENABLED=1` to require auth explicitly.
