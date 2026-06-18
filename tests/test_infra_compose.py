"""
test_infra_compose.py — offline structural tests for T4 infra deliverables.

Validates:
- docker-compose.yml: required services present, each has a healthcheck,
  image tags are pinned for postgres/redis/elasticsearch/grafana.
- infra/servicebus-config.json: five topics present, correct subscriptions,
  duplicate detection enabled on market.raw and signals.

No live Docker daemon or network access required.
"""

import json
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
SB_CONFIG_FILE = REPO_ROOT / "infra" / "servicebus-config.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_compose() -> dict:
    with COMPOSE_FILE.open() as f:
        return yaml.safe_load(f)


def load_sb_config() -> dict:
    with SB_CONFIG_FILE.open() as f:
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
        assert "healthcheck" in svc_def, (
            f"Service '{svc}' has no healthcheck defined"
        )
        hc = svc_def["healthcheck"]
        assert "test" in hc, f"Service '{svc}' healthcheck has no 'test' key"

    @pytest.mark.parametrize("svc,expected_image", PINNED_TAG_SERVICES.items())
    def test_image_tag_pinned(self, services: dict, svc: str, expected_image: str) -> None:
        image = services.get(svc, {}).get("image", "")
        assert image == expected_image, (
            f"Service '{svc}' image should be '{expected_image}', got '{image}'"
        )

    def test_latest_only_for_allowed_services(self, services: dict) -> None:
        """No bare :latest tags for services that must be pinned."""
        for svc, definition in services.items():
            if svc in LATEST_ALLOWED:
                continue
            image = definition.get("image", "")
            # image tags pulled from anchors (druid) may have the tag in the image string
            if image and image.endswith(":latest"):
                pytest.fail(
                    f"Service '{svc}' uses ':latest' image tag — pin it: {image}"
                )

    def test_servicebus_emulator_port_5672(self, services: dict) -> None:
        """AMQP port 5672 is exposed by the emulator."""
        ports = services["servicebus-emulator"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("5672" in p for p in port_strings), (
            "servicebus-emulator must expose port 5672 (AMQP)"
        )

    def test_servicebus_emulator_accepts_eula(self, services: dict) -> None:
        env = services["servicebus-emulator"].get("environment", {})
        assert env.get("ACCEPT_EULA") == "Y", (
            "servicebus-emulator must set ACCEPT_EULA=Y"
        )

    def test_mssql_accepts_eula(self, services: dict) -> None:
        env = services["mssql"].get("environment", {})
        assert env.get("ACCEPT_EULA") == "Y", "mssql must set ACCEPT_EULA=Y"

    def test_druid_exposes_router_port_8888(self, services: dict) -> None:
        ports = services["druid"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("8888" in p for p in port_strings), (
            "Druid must expose port 8888 (router/console)"
        )

    def test_grafana_exposes_port_3000(self, services: dict) -> None:
        ports = services["grafana"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("3000" in p for p in port_strings), (
            "Grafana must expose port 3000"
        )

    def test_servicebus_emulator_depends_on_mssql(self, services: dict) -> None:
        depends = services["servicebus-emulator"].get("depends_on", {})
        if isinstance(depends, list):
            assert "mssql" in depends
        else:
            assert "mssql" in depends, (
                "servicebus-emulator must depend_on mssql"
            )

    def test_sb_config_volume_mount(self, services: dict) -> None:
        """servicebus-emulator mounts the config JSON into the correct container path."""
        volumes = services["servicebus-emulator"].get("volumes", [])
        mount_paths = [str(v) for v in volumes]
        assert any(
            "/ServiceBus_Emulator/ConfigFiles/Config.json" in p for p in mount_paths
        ), (
            "servicebus-emulator must mount config.json to"
            " /ServiceBus_Emulator/ConfigFiles/Config.json"
        )


# ---------------------------------------------------------------------------
# infra/servicebus-config.json — topology
# ---------------------------------------------------------------------------

EXPECTED_TOPICS = {
    "market.raw": {"subscriptions": {"stream", "api"}, "duplicate_detection": True},
    "news.raw": {"subscriptions": {"ai"}, "duplicate_detection": False},
    "signals": {"subscriptions": {"ai", "alerting", "api"}, "duplicate_detection": True},
    "insights": {"subscriptions": {"alerting", "api"}, "duplicate_detection": False},
    "alerts": {"subscriptions": {"api"}, "duplicate_detection": False},
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
        assert topic_name in topics_by_name, (
            f"Topic '{topic_name}' is missing from servicebus-config.json"
        )

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
    def test_duplicate_detection_enabled(
        self, topics_by_name: dict, topic_name: str
    ) -> None:
        props = topics_by_name[topic_name].get("Properties", {})
        assert props.get("RequiresDuplicateDetection") is True, (
            f"Topic '{topic_name}' must have RequiresDuplicateDetection=true"
        )

    @pytest.mark.parametrize(
        "topic_name",
        [t for t, v in EXPECTED_TOPICS.items() if not v["duplicate_detection"]],
    )
    def test_duplicate_detection_not_required_on_others(
        self, topics_by_name: dict, topic_name: str
    ) -> None:
        props = topics_by_name[topic_name].get("Properties", {})
        # RequiresDuplicateDetection should be absent or explicitly false
        assert props.get("RequiresDuplicateDetection", False) is False, (
            f"Topic '{topic_name}' should not have RequiresDuplicateDetection=true"
        )

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
