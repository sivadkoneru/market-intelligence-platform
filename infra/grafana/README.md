# infra/grafana/

Grafana provisioning assets for the local observability stack.

Portfolio project only. No financial advice and no real trades.

This tree is mounted into the Grafana container at `/etc/grafana/provisioning`
by `docker-compose.yml`.

## Contents

- `provisioning/datasources/` - provisioned datasources for Elasticsearch logs
  and Druid SQL over HTTP
- `provisioning/dashboards/` - dashboard provider definition
- `provisioning/dashboards-json/` - dashboard JSON files loaded by the provider

## Runtime shape

- Elasticsearch uses Grafana's native Elasticsearch datasource
- Druid is queried through the pinned JSON datasource plugin against the Druid SQL
  endpoint
- Dashboards are file-provisioned on startup; no manual import is required
