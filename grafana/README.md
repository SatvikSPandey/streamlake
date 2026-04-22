# StreamLake — Grafana BI Dashboards

Self-hosted Grafana OSS with the Michelin Snowflake community plugin, visualizing the Gold-layer tables in Snowflake as OTT analytics dashboards.

## Why self-hosted?

Grafana Cloud free tier has a documented UX limitation where Enterprise plugins (including the official Snowflake plugin) cannot be installed through any documented path (see [grafana/grafana#73157](https://github.com/grafana/grafana/issues/73157)). Self-hosting via Docker with the Apache-licensed [michelin/snowflake-grafana-datasource](https://github.com/michelin/snowflake-grafana-datasource) plugin is the production-realistic workaround and is what most teams actually run for local dev.

## Architecture

Snowflake Gold tables  ─┐
▼
michelin-snowflake-datasource (plugin)
▼
Grafana OSS 11.4 (Docker container)
▼
http://localhost:3000

## Dashboard panels

The `StreamLake OTT Analytics` dashboard contains 5 panels answering OTT product questions:

| Panel | Visualization | Gold table |
|---|---|---|
| Session Summary | 4 stat cards (Total Sessions, Unique Users, Total Events, Avg Watch) | `USER_SESSION_SUMMARY` |
| Device Mix | Pie chart | `DAILY_DEVICE_MIX` |
| Event Type Distribution | Bar chart | `HOURLY_EVENT_DISTRIBUTION` |
| Top Content by Watch Time | Horizontal bar chart (top 10) | `TOP_CONTENT_BY_WATCH_TIME` |
| Daily Watch Time by Country | Bar chart | `DAILY_WATCH_TIME_BY_COUNTRY` |

## Running locally

Prerequisites: Docker Desktop running, Snowflake credentials from `imp/.env`.

```powershell
cd grafana
docker compose up -d
```

Then open http://localhost:3000 — login `admin` / `admin` (change on first login).

To configure the Snowflake data source, use:
- Account name: `<YOUR_ACCOUNT>.snowflakecomputing.com`
- Auth: Password
- Username, Password, Role, Warehouse, Database, Schema: from `imp/.env`

To import the dashboard, navigate to `Dashboards → New → Import`, upload `dashboard.json`, and select the Snowflake data source you just configured.

## Stopping

```powershell
docker compose down
```

Data persists across restarts via the Docker-managed `grafana-storage` volume. To wipe completely:

```powershell
docker compose down -v
```