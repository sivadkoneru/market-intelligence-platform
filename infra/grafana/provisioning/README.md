# Grafana provisioning

Provisioning files for the local Grafana container.

Portfolio project only. No financial advice and no real trades.

## Layout

- `datasources/` - datasource YAML
- `dashboards/` - dashboard provider YAML
- `dashboards-json/` - dashboard JSON definitions

Grafana reads these files at startup from `/etc/grafana/provisioning`.
