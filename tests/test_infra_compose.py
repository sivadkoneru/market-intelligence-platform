"""
test_infra_compose.py — offline structural tests for T4 infra deliverables.

Validates:
- docker-compose.yml: required services present, each has a healthcheck,
  image tags are pinned for postgres/redis/elasticsearch/grafana, and Grafana
  mounts the provisioning tree with the JSON datasource plugin enabled.
- infra/servicebus-config.json: five topics present, correct subscriptions,
  duplicate detection enabled on market.raw and signals.
- infra/grafana/provisioning: datasource and dashboard YAML/JSON are present
  and the dashboard references both Elasticsearch and Druid datasources.

No live Docker daemon or network access required.
"""

import json
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
SB_CONFIG_FILE = REPO_ROOT / "infra" / "servicebus-config.json"
GRAFANA_ROOT = REPO_ROOT / "infra" / "grafana"
GRAFANA_PROVISIONING_DIR = GRAFANA_ROOT / "provisioning"
GRAFANA_DATASOURCES_FILE = GRAFANA_PROVISIONING_DIR / "datasources" / "datasources.yaml"
GRAFANA_DASHBOARDS_PROVIDER_FILE = GRAFANA_PROVISIONING_DIR / "dashboards" / "dashboards.yaml"
GRAFANA_DASHBOARD_FILE = GRAFANA_PROVISIONING_DIR / "dashboards-json" / "market-observability.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_compose() -> dict:
    with COMPOSE_FILE.open() as f:
        return yaml.safe_load(f)


def load_sb_config() -> dict:
    with SB_CONFIG_FILE.open() as f:
        return json.load(f)


def load_yaml(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# docker-compose.yml — required infra services
# ---------------------------------------------------------------------------

REQUIRED_SERVICES = [
    "mssql",
    "servicebus-emulator",
    "postgres",
    "zookeeper",
    "druid",
    "redis",
    "elasticsearch",
    "grafana",
    "ingestion",
    "stream",
    "ai-analysis",
    "alerting",
    "api",
]

# Tags that MUST be pinned (not bare :latest)
PINNED_TAG_SERVICES = {
    "postgres": "postgres:16",
    "redis": "redis:7",
    "elasticsearch": "docker.elastic.co/elasticsearch/elasticsearch:8.17.0",
    "grafana": "grafana/grafana:11.3.0",
    "zookeeper": "zookeeper:3.8",
    "druid": "apache/druid:30.0.0",
}

# SB emulator and mssql are allowed to use :latest per brief
LATEST_ALLOWED = {"mssql", "servicebus-emulator"}

APP_BACKEND_ENV = {
    "SERVICE_BUS_CONNECTION_STRING": (
        "Endpoint=sb://servicebus-emulator;"
        "SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=SAS_KEY_VALUE;"
        "UseDevelopmentEmulator=true;"
    ),
    "REDIS_URL": "redis://redis:6379/0",
    "DRUID_URL": "http://druid:8888",
    "ELASTICSEARCH_URL": "http://elasticsearch:9200",
}

APP_SERVICES = {
    "ingestion": {
        "dockerfile": "services/ingestion/Dockerfile",
        "container_port": "8001",
        "host_port": "8001",
        "depends_on": {"servicebus-emulator", "elasticsearch"},
    },
    "stream": {
        "dockerfile": "services/stream/Dockerfile",
        "container_port": "8002",
        "host_port": "8002",
        "depends_on": {"servicebus-emulator", "redis", "druid", "elasticsearch"},
    },
    "ai-analysis": {
        "dockerfile": "services/ai/Dockerfile",
        "container_port": "8003",
        "host_port": "8003",
        "depends_on": {"servicebus-emulator", "redis", "elasticsearch"},
    },
    "alerting": {
        "dockerfile": "services/alerting/Dockerfile",
        "container_port": "8004",
        "host_port": "8004",
        "depends_on": {"servicebus-emulator", "redis", "elasticsearch"},
    },
    "api": {
        "dockerfile": "services/api/Dockerfile",
        "container_port": "8005",
        "host_port": "8000",
        "depends_on": {"servicebus-emulator", "redis", "druid", "elasticsearch"},
    },
}


@pytest.fixture(scope="module")
def compose() -> dict:
    assert COMPOSE_FILE.exists(), f"docker-compose.yml not found at {COMPOSE_FILE}"
    return load_compose()


@pytest.fixture(scope="module")
def sb_config() -> dict:
    assert SB_CONFIG_FILE.exists(), f"SB config not found at {SB_CONFIG_FILE}"
    return load_sb_config()


@pytest.fixture(scope="module")
def services(compose: dict) -> dict:
    return compose.get("services", {})


class TestComposeServices:
    """All required infrastructure services are defined."""

    @pytest.mark.parametrize("svc", REQUIRED_SERVICES)
    def test_service_present(self, services: dict, svc: str) -> None:
        assert svc in services, f"Service '{svc}' is missing from docker-compose.yml"

    @pytest.mark.parametrize("svc", REQUIRED_SERVICES)
    def test_service_has_healthcheck(self, services: dict, svc: str) -> None:
        svc_def = services.get(svc, {})
        assert "healthcheck" in svc_def, f"Service '{svc}' has no healthcheck defined"
        hc = svc_def["healthcheck"]
        assert "test" in hc, f"Service '{svc}' healthcheck has no 'test' key"

    @pytest.mark.parametrize("svc,expected_image", PINNED_TAG_SERVICES.items())
    def test_image_tag_pinned(self, services: dict, svc: str, expected_image: str) -> None:
        image = services.get(svc, {}).get("image", "")
        assert (
            image == expected_image
        ), f"Service '{svc}' image should be '{expected_image}', got '{image}'"

    def test_latest_only_for_allowed_services(self, services: dict) -> None:
        """No bare :latest tags for services that must be pinned."""
        for svc, definition in services.items():
            if svc in LATEST_ALLOWED:
                continue
            image = definition.get("image", "")
            # image tags pulled from anchors (druid) may have the tag in the image string
            if image and image.endswith(":latest"):
                pytest.fail(f"Service '{svc}' uses ':latest' image tag — pin it: {image}")

    def test_servicebus_emulator_port_5672(self, services: dict) -> None:
        """AMQP port 5672 is exposed by the emulator."""
        ports = services["servicebus-emulator"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any(
            "5672" in p for p in port_strings
        ), "servicebus-emulator must expose port 5672 (AMQP)"

    def test_servicebus_emulator_accepts_eula(self, services: dict) -> None:
        env = services["servicebus-emulator"].get("environment", {})
        assert env.get("ACCEPT_EULA") == "Y", "servicebus-emulator must set ACCEPT_EULA=Y"

    def test_mssql_accepts_eula(self, services: dict) -> None:
        env = services["mssql"].get("environment", {})
        assert env.get("ACCEPT_EULA") == "Y", "mssql must set ACCEPT_EULA=Y"

    def test_druid_exposes_router_port_8888(self, services: dict) -> None:
        ports = services["druid"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any(
            "8888" in p for p in port_strings
        ), "Druid must expose port 8888 (router/console)"

    def test_grafana_exposes_port_3000(self, services: dict) -> None:
        ports = services["grafana"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("3000" in p for p in port_strings), "Grafana must expose port 3000"

    def test_grafana_mounts_provisioning_tree(self, services: dict) -> None:
        volumes = services["grafana"].get("volumes", [])
        mount_paths = [str(v) for v in volumes]
        assert any(
            "/etc/grafana/provisioning" in p for p in mount_paths
        ), "grafana must mount /etc/grafana/provisioning"

    def test_grafana_installs_json_datasource_plugin(self, services: dict) -> None:
        env = services["grafana"].get("environment", {})
        plugins = env.get("GF_PLUGINS_PREINSTALL", "")
        assert (
            plugins == "marcusolsson-json-datasource@1.3.24"
        ), "grafana must preinstall a pinned marcusolsson-json-datasource plugin"

    def test_servicebus_emulator_depends_on_mssql(self, services: dict) -> None:
        depends = services["servicebus-emulator"].get("depends_on", {})
        if isinstance(depends, list):
            assert "mssql" in depends
        else:
            assert "mssql" in depends, "servicebus-emulator must depend_on mssql"

    def test_sb_config_volume_mount(self, services: dict) -> None:
        """servicebus-emulator mounts the config JSON into the correct container path."""
        volumes = services["servicebus-emulator"].get("volumes", [])
        mount_paths = [str(v) for v in volumes]
        assert any("/ServiceBus_Emulator/ConfigFiles/Config.json" in p for p in mount_paths), (
            "servicebus-emulator must mount config.json to"
            " /ServiceBus_Emulator/ConfigFiles/Config.json"
        )

    @pytest.mark.parametrize("svc,expected", APP_SERVICES.items())
    def test_app_service_build_context_and_dockerfile(
        self, services: dict, svc: str, expected: dict
    ) -> None:
        build = services[svc].get("build", {})
        assert build.get("context") == "."
        assert build.get("dockerfile") == expected["dockerfile"]

    @pytest.mark.parametrize("svc,expected", APP_SERVICES.items())
    def test_app_service_port_mapping(self, services: dict, svc: str, expected: dict) -> None:
        ports = [str(port) for port in services[svc].get("ports", [])]
        assert f'{expected["host_port"]}:{expected["container_port"]}' in ports

    @pytest.mark.parametrize("svc,expected", APP_SERVICES.items())
    def test_app_service_depends_on_required_backends(
        self, services: dict, svc: str, expected: dict
    ) -> None:
        depends_on = services[svc].get("depends_on", {})
        keys = set(depends_on if isinstance(depends_on, list) else depends_on.keys())
        assert expected["depends_on"].issubset(keys)

    @pytest.mark.parametrize("svc", APP_SERVICES.keys())
    def test_app_services_restart_unless_stopped(self, services: dict, svc: str) -> None:
        assert services[svc].get("restart") == "unless-stopped"

    @pytest.mark.parametrize("svc", APP_SERVICES.keys())
    def test_app_services_join_main_network(self, services: dict, svc: str) -> None:
        networks = services[svc].get("networks", [])
        assert "mip-net" in networks

    @pytest.mark.parametrize("svc", APP_SERVICES.keys())
    def test_app_services_export_runtime_env(self, services: dict, svc: str) -> None:
        env = services[svc].get("environment", {})
        for key, expected in APP_BACKEND_ENV.items():
            assert env[key] == expected
        assert "POSTGRES_DSN" in env
        assert "@postgres:5432/" in env["POSTGRES_DSN"]
        assert env["MOCK_LLM"] == "${MOCK_LLM:-1}"

    def test_api_exposes_host_port_8000(self, services: dict) -> None:
        ports = [str(port) for port in services["api"].get("ports", [])]
        assert "8000:8005" in ports

    def test_druid_metadata_store_uses_dedicated_env_vars(self, services: dict) -> None:
        env = services["druid"].get("environment", {})
        assert env["druid_metadata_storage_connector_connectURI"] == (
            "jdbc:postgresql://postgres:5432/${DRUID_POSTGRES_DB:-mip}"
        )
        assert env["druid_metadata_storage_connector_user"] == "${DRUID_POSTGRES_USER:-mip}"
        assert env["druid_metadata_storage_connector_password"] == (
            "${DRUID_POSTGRES_PASSWORD:-mip_local}"
        )


# ---------------------------------------------------------------------------
# infra/servicebus-config.json — topology
# ---------------------------------------------------------------------------

EXPECTED_TOPICS = {
    "market.raw": {
        "subscriptions": {"stream", "api", "api-ws"},
        "duplicate_detection": True,
    },
    "news.raw": {"subscriptions": {"ai"}, "duplicate_detection": False},
    "signals": {
        "subscriptions": {"ai", "alerting", "api", "api-ws"},
        "duplicate_detection": True,
    },
    "insights": {
        "subscriptions": {"alerting", "api", "api-ws"},
        "duplicate_detection": False,
    },
    "alerts": {"subscriptions": {"api", "api-ws"}, "duplicate_detection": False},
}


class TestServiceBusConfig:
    """SB emulator config defines the correct namespace, topics, and subscriptions."""

    @pytest.fixture(scope="class")
    def namespace(self, sb_config: dict) -> dict:
        namespaces = sb_config["UserConfig"]["Namespaces"]
        assert len(namespaces) >= 1, "Expected at least one namespace in SB config"
        return namespaces[0]

    @pytest.fixture(scope="class")
    def topics_by_name(self, namespace: dict) -> dict:
        return {t["Name"]: t for t in namespace.get("Topics", [])}

    @pytest.mark.parametrize("topic_name", EXPECTED_TOPICS.keys())
    def test_topic_present(self, topics_by_name: dict, topic_name: str) -> None:
        assert (
            topic_name in topics_by_name
        ), f"Topic '{topic_name}' is missing from servicebus-config.json"

    @pytest.mark.parametrize("topic_name,expected", EXPECTED_TOPICS.items())
    def test_topic_subscriptions(
        self, topics_by_name: dict, topic_name: str, expected: dict
    ) -> None:
        topic = topics_by_name.get(topic_name, {})
        actual_subs = {s["Name"] for s in topic.get("Subscriptions", [])}
        assert actual_subs == expected["subscriptions"], (
            f"Topic '{topic_name}' subscriptions mismatch. "
            f"Expected {expected['subscriptions']}, got {actual_subs}"
        )

    @pytest.mark.parametrize(
        "topic_name",
        [t for t, v in EXPECTED_TOPICS.items() if v["duplicate_detection"]],
    )
    def test_duplicate_detection_enabled(self, topics_by_name: dict, topic_name: str) -> None:
        props = topics_by_name[topic_name].get("Properties", {})
        assert (
            props.get("RequiresDuplicateDetection") is True
        ), f"Topic '{topic_name}' must have RequiresDuplicateDetection=true"

    @pytest.mark.parametrize(
        "topic_name",
        [t for t, v in EXPECTED_TOPICS.items() if not v["duplicate_detection"]],
    )
    def test_duplicate_detection_not_required_on_others(
        self, topics_by_name: dict, topic_name: str
    ) -> None:
        props = topics_by_name[topic_name].get("Properties", {})
        # RequiresDuplicateDetection should be absent or explicitly false
        assert (
            props.get("RequiresDuplicateDetection", False) is False
        ), f"Topic '{topic_name}' should not have RequiresDuplicateDetection=true"

    def test_all_subscriptions_have_dead_lettering(self, topics_by_name: dict) -> None:
        """All subscriptions should enable DeadLetteringOnMessageExpiration."""
        for topic_name, topic in topics_by_name.items():
            for sub in topic.get("Subscriptions", []):
                props = sub.get("Properties", {})
                assert props.get("DeadLetteringOnMessageExpiration") is True, (
                    f"Subscription '{sub['Name']}' on topic '{topic_name}' "
                    f"should have DeadLetteringOnMessageExpiration=true"
                )

    def test_five_topics_total(self, topics_by_name: dict) -> None:
        assert len(topics_by_name) == 5, (
            f"Expected exactly 5 topics, found {len(topics_by_name)}: "
            f"{list(topics_by_name.keys())}"
        )


# ---------------------------------------------------------------------------
# infra/grafana/provisioning — datasources and dashboards
# ---------------------------------------------------------------------------


class TestGrafanaProvisioning:
    """Grafana provisioning files exist and wire both observability datasources."""

    @pytest.fixture(scope="class")
    def grafana_root(self) -> Path:
        assert GRAFANA_ROOT.exists(), f"Grafana tree not found at {GRAFANA_ROOT}"
        return GRAFANA_ROOT

    @pytest.fixture(scope="class")
    def grafana_provisioning(self, grafana_root: Path) -> Path:
        assert (
            GRAFANA_PROVISIONING_DIR.exists()
        ), f"Grafana provisioning dir not found at {GRAFANA_PROVISIONING_DIR}"
        return GRAFANA_PROVISIONING_DIR

    @pytest.fixture(scope="class")
    def datasources(self) -> dict:
        assert (
            GRAFANA_DATASOURCES_FILE.exists()
        ), f"Grafana datasource file not found at {GRAFANA_DATASOURCES_FILE}"
        return load_yaml(GRAFANA_DATASOURCES_FILE)

    @pytest.fixture(scope="class")
    def dashboards_provider(self) -> dict:
        assert (
            GRAFANA_DASHBOARDS_PROVIDER_FILE.exists()
        ), f"Grafana dashboard provider not found at {GRAFANA_DASHBOARDS_PROVIDER_FILE}"
        return load_yaml(GRAFANA_DASHBOARDS_PROVIDER_FILE)

    @pytest.fixture(scope="class")
    def dashboard(self) -> dict:
        assert (
            GRAFANA_DASHBOARD_FILE.exists()
        ), f"Grafana dashboard not found at {GRAFANA_DASHBOARD_FILE}"
        return load_json(GRAFANA_DASHBOARD_FILE)

    def test_grafana_readme_exists(self, grafana_root: Path) -> None:
        readme = grafana_root / "README.md"
        assert readme.exists(), "infra/grafana/README.md missing"
        assert "No financial advice" in readme.read_text(encoding="utf-8")

    def test_provisioning_readme_exists(self, grafana_provisioning: Path) -> None:
        readme = grafana_provisioning / "README.md"
        assert readme.exists(), "infra/grafana/provisioning/README.md missing"
        assert "No financial advice" in readme.read_text(encoding="utf-8")

    def test_datasource_readme_exists(self) -> None:
        readme = GRAFANA_PROVISIONING_DIR / "datasources" / "README.md"
        assert readme.exists(), "infra/grafana/provisioning/datasources/README.md missing"
        assert "No financial advice" in readme.read_text(encoding="utf-8")

    def test_dashboards_readme_exists(self) -> None:
        readme = GRAFANA_PROVISIONING_DIR / "dashboards" / "README.md"
        assert readme.exists(), "infra/grafana/provisioning/dashboards/README.md missing"
        assert "No financial advice" in readme.read_text(encoding="utf-8")

    def test_dashboard_json_readme_exists(self) -> None:
        readme = GRAFANA_PROVISIONING_DIR / "dashboards-json" / "README.md"
        assert readme.exists(), "infra/grafana/provisioning/dashboards-json/README.md missing"
        assert "No financial advice" in readme.read_text(encoding="utf-8")

    def test_datasource_file_declares_elasticsearch_and_druid(self, datasources: dict) -> None:
        entries = datasources.get("datasources", [])
        assert len(entries) == 2, f"Expected 2 datasources, found {len(entries)}"
        by_name = {entry["name"]: entry for entry in entries}
        assert "Elasticsearch Logs" in by_name
        assert "Druid HTTP" in by_name
        assert by_name["Elasticsearch Logs"]["type"] == "elasticsearch"
        assert by_name["Druid HTTP"]["type"] == "marcusolsson-json-datasource"
        assert by_name["Elasticsearch Logs"]["url"] == "http://elasticsearch:9200"
        assert by_name["Druid HTTP"]["url"] == "http://druid:8888/druid/v2/sql"
        assert by_name["Elasticsearch Logs"]["jsonData"]["timeField"] == "timestamp"
        assert by_name["Elasticsearch Logs"]["jsonData"]["logMessageField"] == "event"

    def test_dashboard_provider_points_at_dashboard_json(self, dashboards_provider: dict) -> None:
        providers = dashboards_provider.get("providers", [])
        assert len(providers) == 1, f"Expected one dashboard provider, got {len(providers)}"
        provider = providers[0]
        assert provider["options"]["path"] == "/etc/grafana/provisioning/dashboards-json"
        assert provider["type"] == "file"
        assert provider["folder"] == "Market Intelligence"

    def test_dashboard_uses_both_datasources(self, dashboard: dict) -> None:
        panels = dashboard.get("panels", [])
        assert len(panels) >= 4, "Expected at least four dashboard panels"

        datasource_uids = {
            panel.get("datasource", {}).get("uid")
            for panel in panels
            if isinstance(panel.get("datasource"), dict)
        }
        assert (
            "mip-elasticsearch-logs" in datasource_uids
        ), "Dashboard must reference the Elasticsearch logs datasource"
        assert (
            "mip-druid-http" in datasource_uids
        ), "Dashboard must reference the Druid HTTP datasource"

        panel_titles = {panel.get("title") for panel in panels}
        assert "Elasticsearch log volume" in panel_titles
        assert "Druid market and indicator rows" in panel_titles
        assert "Druid indicator volatility" in panel_titles

    def test_dashboard_panel_queries_cover_logs_and_druid(self, dashboard: dict) -> None:
        panels = dashboard.get("panels", [])
        elastic_queries = []
        druid_queries = []
        for panel in panels:
            datasource = panel.get("datasource", {})
            for target in panel.get("targets", []):
                if datasource.get("uid") == "mip-elasticsearch-logs":
                    elastic_queries.append(target)
                if datasource.get("uid") == "mip-druid-http":
                    druid_queries.append(target)

        assert any(
            target.get("query") == "*" for target in elastic_queries
        ), "Expected an Elasticsearch query over the log index"
        assert all(
            target.get("timeField") == "timestamp"
            for target in elastic_queries
            if target.get("timeField")
        ), "Elasticsearch panels must use the structlog timestamp field"
        assert any(
            bucket.get("field") == "service"
            for target in elastic_queries
            for bucket in target.get("bucketAggs", [])
        ), "Expected a service terms aggregation over the structlog service field"
        assert any(
            "SELECT TIME_FLOOR(" in target.get("data", {}).get("query", "")
            for target in druid_queries
        ), "Expected a Druid SQL query in the dashboard"
        assert all(
            "${symbol}" in target.get("data", {}).get("query", "") for target in druid_queries
        ), "Druid SQL panels must use the provisioned symbol variable"
