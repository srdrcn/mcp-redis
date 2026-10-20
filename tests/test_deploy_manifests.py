"""
Consistency tests for the OpenShift manifests in deploy/openshift.

The manifests reference each other by name (ConfigMap, Secrets, keys, mount paths) and the ConfigMap embeds a
redis-mcp.yaml that must stay a valid config, equal in its clusters to redis-mcp.example.yaml. Nothing else checks
these files before an `oc apply`, so a rename or an edit in one place is caught here.
"""

import re
from pathlib import Path

import pytest
import yaml

from src.common.config_loader import (
    ConfigError,
    ServerConfig,
    api_key_error,
    load_config,
)
from src.main import apply_api_key

ROOT = Path(__file__).resolve().parent.parent
DEPLOY_DIR = ROOT / "deploy" / "openshift"
EXAMPLE_CONFIG = ROOT / "redis-mcp.example.yaml"

API_KEY_ENV = "REDIS_MCP_API_KEY"
CONFIG_KEY = "redis-mcp.yaml"
EXPECTED_CLUSTERS = ["cbdevrds", "cbtstrds", "cbprdrds", "cbsbxrds"]
PLACEHOLDER = re.compile(r"<[^<>]+>")

# The repository's manifests; git-ignored *.local.yaml copies (the filled-in Secrets) are the user's own.
MANIFESTS = sorted(
    p
    for p in DEPLOY_DIR.rglob("*")
    if p.is_file()
    and p.suffix in (".yaml", ".yml")
    and not p.name.endswith((".local.yaml", ".local.yml"))
)


def load_documents(name: str) -> list:
    with (DEPLOY_DIR / name).open(encoding="utf-8") as f:
        return [doc for doc in yaml.safe_load_all(f) if doc is not None]


def load_single(name: str) -> dict:
    documents = load_documents(name)
    assert len(documents) == 1, f"{name} should hold exactly one document"
    return documents[0]


def by_name(items: list, name: str) -> dict:
    matches = [item for item in items if item.get("name") == name]
    assert len(matches) == 1, f"expected exactly one entry named {name}"
    return matches[0]


@pytest.fixture(scope="module")
def deployment() -> dict:
    doc = load_single("deployment.yaml")
    assert doc["kind"] == "Deployment"
    return doc


@pytest.fixture(scope="module")
def pod_spec(deployment) -> dict:
    return deployment["spec"]["template"]["spec"]


@pytest.fixture(scope="module")
def container(pod_spec) -> dict:
    return by_name(pod_spec["containers"], "redis-mcp")


@pytest.fixture(scope="module")
def config_volume(pod_spec) -> dict:
    return by_name(pod_spec["volumes"], "config")["configMap"]


@pytest.fixture(scope="module")
def passwords_volume(pod_spec) -> dict:
    return by_name(pod_spec["volumes"], "passwords")["secret"]


@pytest.fixture(scope="module")
def api_key_ref(container) -> dict:
    env = by_name(container.get("env", []), API_KEY_ENV)
    return env["valueFrom"]["secretKeyRef"]


@pytest.fixture(scope="module")
def configmap() -> dict:
    return load_single("configmap.yaml")


@pytest.fixture
def configmap_config(configmap, tmp_path) -> ServerConfig:
    path = tmp_path / CONFIG_KEY
    path.write_text(configmap["data"][CONFIG_KEY], encoding="utf-8")
    return load_config(path)


@pytest.fixture(scope="module")
def secrets() -> list:
    return load_documents("secret.yaml")


def secret_named(secrets: list, name: str) -> dict:
    matches = [s for s in secrets if s["metadata"]["name"] == name]
    assert len(matches) == 1, f"secret.yaml should hold exactly one Secret named {name}"
    return matches[0]


def test_manifests_found():
    names = {p.name for p in MANIFESTS}
    assert {
        "kustomization.yaml",
        "configmap.yaml",
        "deployment.yaml",
        "secret.yaml",
    } <= names


@pytest.mark.parametrize(
    "path", MANIFESTS, ids=[str(p.relative_to(DEPLOY_DIR)) for p in MANIFESTS]
)
def test_manifest_parses(path):
    with path.open(encoding="utf-8") as f:
        documents = [doc for doc in yaml.safe_load_all(f) if doc is not None]
    assert documents
    for doc in documents:
        assert isinstance(doc, dict)
        assert doc.get("apiVersion")
        assert doc.get("kind")


class TestKustomization:
    @pytest.fixture(scope="class")
    def resources(self) -> list:
        doc = load_single("kustomization.yaml")
        assert doc["kind"] == "Kustomization"
        return doc["resources"]

    def test_resources_exist(self, resources):
        for resource in resources:
            assert (DEPLOY_DIR / resource).is_file(), resource

    def test_configmap_is_a_resource(self, resources):
        assert "configmap.yaml" in resources

    def test_secret_is_not_a_resource(self, resources):
        # the placeholders must never be applied by `oc apply -k`: users apply a filled-in secret.local.yaml
        assert "secret.yaml" not in resources
        assert not [r for r in resources if ".local." in r]

    def test_every_other_manifest_is_a_resource(self, resources):
        others = {
            str(p.relative_to(DEPLOY_DIR))
            for p in MANIFESTS
            if p.name not in ("kustomization.yaml", "secret.yaml")
        }
        assert others == set(resources)


class TestConfigMap:
    def test_named_as_the_config_volume(self, configmap, config_volume):
        assert configmap["kind"] == "ConfigMap"
        assert configmap["metadata"]["name"] == config_volume["name"]

    def test_holds_the_mounted_key(self, configmap, config_volume):
        mounted = [item["key"] for item in config_volume["items"]]
        assert mounted == [CONFIG_KEY]
        assert set(configmap["data"]) == {CONFIG_KEY}

    def test_config_path_is_the_mounted_file(self, container, config_volume):
        mount = by_name(container["volumeMounts"], "config")
        item = next(i for i in config_volume["items"] if i["key"] == CONFIG_KEY)
        args = container["args"]
        config_path = args[args.index("--config") + 1]
        assert config_path == f"{mount['mountPath']}/{item['path']}"

    def test_embedded_config_loads(self, configmap_config):
        # no apiKey: the key comes from the redis-mcp-api-key Secret through REDIS_MCP_API_KEY
        assert configmap_config.api_key is None
        assert configmap_config.cluster_names() == EXPECTED_CLUSTERS

    def test_passwords_dir_is_the_mounted_secret(self, configmap_config, container):
        mount = by_name(container["volumeMounts"], "passwords")
        assert configmap_config.passwords_dir == mount["mountPath"]

    def test_clusters_match_the_example_config(self, configmap_config, tmp_path):
        # The example sets apiKey: <api-key>, which the loader rejects; a drop-in clears it so the example's
        # clusters can be compared. The two examples must not drift apart.
        drop_in = tmp_path / "drop-in"
        drop_in.mkdir()
        (drop_in / "api-key.yaml").write_text('apiKey: ""\n', encoding="utf-8")
        example = load_config(EXAMPLE_CONFIG, drop_in)
        assert [c.model_dump() for c in configmap_config.clusters] == [
            c.model_dump() for c in example.clusters
        ]

    def test_example_config_placeholder_is_rejected(self):
        with pytest.raises(ConfigError, match="placeholder"):
            load_config(EXAMPLE_CONFIG)


class TestSecrets:
    def test_two_opaque_secrets(self, secrets):
        assert len(secrets) == 2
        for secret in secrets:
            assert secret["kind"] == "Secret"
            assert secret["type"] == "Opaque"
            # only stringData, so every value is checked below as written
            assert "data" not in secret

    def test_names_match_the_deployment(self, secrets, passwords_volume, api_key_ref):
        assert {s["metadata"]["name"] for s in secrets} == {
            passwords_volume["secretName"],
            api_key_ref["name"],
        }

    def test_password_keys_match_the_configmap_clusters(
        self, secrets, passwords_volume, configmap_config
    ):
        passwords = secret_named(secrets, passwords_volume["secretName"])
        assert set(passwords["stringData"]) == {
            name + "_pass" for name in configmap_config.cluster_names()
        }

    def test_api_key_secret_holds_the_referenced_key(self, secrets, api_key_ref):
        api_key = secret_named(secrets, api_key_ref["name"])
        assert set(api_key["stringData"]) == {api_key_ref["key"]}

    def test_all_values_are_placeholders(self, secrets):
        for secret in secrets:
            for key, value in secret["stringData"].items():
                assert isinstance(value, str)
                assert PLACEHOLDER.fullmatch(value), (
                    f"{secret['metadata']['name']}.{key} is not a <...> placeholder"
                )

    def test_unfilled_api_key_stops_the_server(
        self, secrets, api_key_ref, configmap_config
    ):
        value = secret_named(secrets, api_key_ref["name"])["stringData"][
            api_key_ref["key"]
        ]
        assert api_key_error(value) is not None
        # what the server does with REDIS_MCP_API_KEY at startup
        with pytest.raises(ConfigError, match="placeholder"):
            apply_api_key(configmap_config, value)


class TestDeployment:
    def test_api_key_env_is_required(self, api_key_ref):
        # not optional: without the Secret the pod does not start, rather than serving without authentication
        assert api_key_ref["name"] == "redis-mcp-api-key"
        assert api_key_ref["key"] == "api-key"
        assert api_key_ref.get("optional", False) is False
