import os
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from src.orchestrator.docker_service import DockerService
from src.utils.schema_loader import SchemaLoadError

class TestDockerService(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.service = DockerService(client=self.mock_client)

    def test_build_image(self):
        self.service.build_image(tag="test-tag", dockerfile="Test.Dockerfile")
        self.mock_client.images.build.assert_called_once_with(
            path=".", dockerfile="Test.Dockerfile", tag="test-tag", rm=True
        )

    def test_run_node_loader(self):
        mock_container = MagicMock()
        self.mock_client.containers.run.return_value = mock_container

        with tempfile.TemporaryDirectory() as rejection_dir:
            containers = self.service.run_node_loader(
                node_label="Person",
                topic="person_topic",
                mode="stream",
                config_path="config.yaml",
                replicas=2,
                network="test_net",
                rejection_dir=rejection_dir,
            )

        self.assertEqual(len(containers), 2)
        self.assertEqual(self.mock_client.containers.run.call_count, 2)
        
        # Replicas launch concurrently; select replica 1's call explicitly.
        call_kwargs = next(
            call.kwargs for call in self.mock_client.containers.run.call_args_list
            if call.kwargs["labels"]["replica_id"] == "1"
        )
        self.assertEqual(call_kwargs["image"], "graph-loader-node:latest")
        self.assertTrue(call_kwargs["name"].startswith("graph-loader-node-Person-"))
        self.assertEqual(call_kwargs["command"], ["--config", "/app/runtime_schema.yaml", "--node-label", "Person", "--mode", "stream", "--topic", "person_topic", "--replica-id", "1"])
        self.assertEqual(call_kwargs["network"], "test_net")
        self.assertIs(call_kwargs["remove"], True)  # stream loaders auto-remove
        self.assertIn("NEO4J_URI", call_kwargs["environment"])
        self.assertEqual(call_kwargs["labels"]["node_label"], "Person")
        self.assertIn("volumes", call_kwargs)
        # Check volume is bound correctly (ignoring the exact host path)
        volumes = call_kwargs["volumes"]
        self.assertEqual(len(volumes), 2)
        mounts_by_bind = {mount["bind"]: mount for mount in volumes.values()}
        self.assertEqual(mounts_by_bind["/app/runtime_schema.yaml"]["mode"], "ro")
        self.assertEqual(mounts_by_bind["/app/rejections"]["mode"], "rw")
        self.assertEqual(
            call_kwargs["environment"]["REJECTION_LOG_PATH"],
            "/app/rejections/Person-1.jsonl",
        )

    def test_run_node_loader_publishes_metrics_port_and_labels(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_node_loader(
                node_label="Person", topic="person_topic", mode="stream",
                config_path="config.yaml", replicas=1, network="test_net",
                rejection_dir=rejection_dir,
            )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9464"
        assert call["labels"]["graph_loader.kind"] == "node"

    def test_run_node_loader_custom_metrics_port_env_passthrough(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {"GRAPH_LOADER_METRICS_PORT": "9465", "GRAPH_LOADER_METRICS_ENABLED": "0"}):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="stream",
                    config_path="config.yaml", replicas=1, network="test_net",
                    rejection_dir=rejection_dir,
                )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["ports"] == {"9465/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9465"
        assert call["environment"]["GRAPH_LOADER_METRICS_PORT"] == "9465"
        assert call["environment"]["GRAPH_LOADER_METRICS_ENABLED"] == "0"

    def test_run_node_loader_forwards_snapshot_interval_but_not_snapshot_path(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {
            "GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S": "5",
            "GRAPH_LOADER_METRICS_SNAPSHOT_PATH": "/host/only.prom",
        }):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="stream",
                    config_path="config.yaml", replicas=1, network="test_net",
                    rejection_dir=rejection_dir,
                )
        env = self.mock_client.containers.run.call_args.kwargs["environment"]
        assert env["GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S"] == "5"
        assert env.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH") != "/host/only.prom"

    def test_run_node_loader_zero_metrics_port_env_falls_back_to_default(self):
        """P6-S2 review finding F1: '0' parses as a valid int but is not a
        valid port to publish/label - it must fall back, not reach Docker's
        own port-mapping API (which would reject it and fail every launch
        in the run)."""
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {"GRAPH_LOADER_METRICS_PORT": "0"}):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="stream",
                    config_path="config.yaml", replicas=1, network="test_net",
                    rejection_dir=rejection_dir,
                )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9464"

    def test_run_node_loader_out_of_range_metrics_port_env_falls_back_to_default(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {"GRAPH_LOADER_METRICS_PORT": "70000"}):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="stream",
                    config_path="config.yaml", replicas=1, network="test_net",
                    rejection_dir=rejection_dir,
                )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9464"

    def test_run_node_loader_explicit_environment_override_does_not_leak_host_metrics_port(self):
        """P6-S2 review finding F2: when a caller passes an explicit
        environment= that omits the metrics vars, the container will NOT
        see the host's GRAPH_LOADER_METRICS_PORT (explicit environment=
        bypasses _metrics_env_passthrough entirely) - so the published
        port/label must reflect the container's own default (9464), not
        the host's value, even though the host has a different value set."""
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {"GRAPH_LOADER_METRICS_PORT": "9465", "GRAPH_LOADER_METRICS_ENABLED": "0"}):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="stream",
                    config_path="config.yaml", replicas=1, network="test_net",
                    rejection_dir=rejection_dir,
                    environment={"NEO4J_URI": "bolt://custom:7687"},
                )
        call = self.mock_client.containers.run.call_args.kwargs
        # The container never receives the host's metrics vars.
        assert "GRAPH_LOADER_METRICS_PORT" not in call["environment"]
        assert "GRAPH_LOADER_METRICS_ENABLED" not in call["environment"]
        # So the published port/labels must match what the container will
        # actually do (bind 9464, metrics enabled), not the host's values.
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9464"
        assert call["labels"]["graph_loader.metrics_enabled"] == "1"

    def test_run_node_loader_two_replicas_never_collide_on_metrics_port(self):
        self.mock_client.containers.run.side_effect = [MagicMock(), MagicMock()]
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_node_loader(
                node_label="Person", topic="person_topic", mode="stream",
                config_path="config.yaml", replicas=2, network="test_net",
                rejection_dir=rejection_dir,
            )
        calls = self.mock_client.containers.run.call_args_list
        assert len(calls) == 2
        for call in calls:
            assert call.kwargs["ports"] == {"9464/tcp": ("127.0.0.1", None)}
            # Docker assigns the actual host port at ("127.0.0.1", None) —
            # both replicas requesting the identical internal port mapping
            # is exactly the pattern that gets each an independent ephemeral
            # host port with no collision (proven by an integration test
            # elsewhere; this unit test only proves the mapping shape).

    def test_bulk_run_scopes_every_replica_to_its_run(self):
        self.mock_client.containers.run.side_effect = [MagicMock(), MagicMock()]

        with tempfile.TemporaryDirectory() as rejection_dir:
            containers = self.service.run_node_loader(
                node_label="Person",
                topic="person_topic",
                mode="bulk",
                config_path="config.yaml",
                replicas=2,
                network="test_net",
                rejection_dir=rejection_dir,
                run_id="run-ABC_123",
            )

        self.assertEqual(len(containers), 2)
        calls = sorted(
            self.mock_client.containers.run.call_args_list,
            key=lambda call: call.kwargs["labels"]["replica_id"],
        )
        for replica_id, call in enumerate(calls):
            kwargs = call.kwargs
            self.assertEqual(
                kwargs["name"],
                f"graph-loader-node-Person-{replica_id}-{('run-ABC_123').encode().hex()}",
            )
            self.assertEqual(kwargs["environment"]["GRAPH_LOADER_RUN_ID"], "run-ABC_123")
            self.assertEqual(kwargs["labels"]["run_id"], "run-ABC_123")
            self.assertEqual(kwargs["labels"]["replica_id"], str(replica_id))
            self.assertEqual(
                kwargs["command"][-4:],
                ["--replica-id", str(replica_id), "--run-id", "run-ABC_123"],
            )

    def test_bulk_name_encoding_keeps_distinct_ids_distinct(self):
        self.mock_client.containers.run.side_effect = [MagicMock(), MagicMock()]

        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_node_loader(
                node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                network="test_net", rejection_dir=rejection_dir, run_id="a-b",
            )
            self.service.run_node_loader(
                node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                network="test_net", rejection_dir=rejection_dir, run_id="ab",
            )

        first_name = self.mock_client.containers.run.call_args_list[0].kwargs["name"]
        second_name = self.mock_client.containers.run.call_args_list[1].kwargs["name"]
        self.assertNotEqual(first_name, second_name)

    def test_loader_containers_use_internal_endpoints_not_host_endpoints(self):
        self.mock_client.containers.run.return_value = MagicMock()

        with patch.dict(
            "os.environ",
            {"NEO4J_URI": "bolt://localhost:7687", "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"},
            clear=True,
        ):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                    network="test_net", rejection_dir=rejection_dir,
                )

        environment = self.mock_client.containers.run.call_args.kwargs["environment"]
        self.assertEqual(environment["NEO4J_URI"], "bolt://neo4j:7687")
        self.assertEqual(environment["KAFKA_BOOTSTRAP_SERVERS"], "kafka:29092")

    def test_configured_database_reaches_node_and_edge_containers(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with patch.dict("os.environ", {"NEO4J_DATABASE": "latesttesttemp"}, clear=True):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="bulk",
                    config_path="config.yaml", rejection_dir=rejection_dir,
                )
                node_env = self.mock_client.containers.run.call_args.kwargs["environment"]
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                    config_path="config.yaml", rejection_dir=rejection_dir,
                )
                edge_env = self.mock_client.containers.run.call_args.kwargs["environment"]

        self.assertEqual(node_env["NEO4J_DATABASE"], "latesttesttemp")
        self.assertEqual(edge_env["NEO4J_DATABASE"], "latesttesttemp")

    def test_stop_node_loaders(self):
        mock_container_1 = MagicMock()
        mock_container_2 = MagicMock()
        self.mock_client.containers.list.return_value = [mock_container_1, mock_container_2]

        self.service.stop_node_loaders(node_label="Person")
        
        self.mock_client.containers.list.assert_called_once_with(
            all=True, filters={"label": ["component=node-loader", "node_label=Person"]}
        )
        mock_container_1.stop.assert_called_once()
        mock_container_2.stop.assert_called_once()

    def test_list_run_containers_defaults_to_the_universal_app_label(self):
        containers = [MagicMock(), MagicMock()]
        self.mock_client.containers.list.return_value = containers

        self.assertEqual(self.service.list_run_containers(), containers)
        self.mock_client.containers.list.assert_called_once_with(
            all=True, filters={"label": ["app=graph-loader"]}
        )

    def test_list_run_containers_narrows_by_every_given_label(self):
        self.mock_client.containers.list.return_value = []

        self.assertEqual(
            self.service.list_run_containers(label_filters=("app=graph-loader", "run_id=r1")), [],
        )
        self.mock_client.containers.list.assert_called_once_with(
            all=True, filters={"label": ["app=graph-loader", "run_id=r1"]}
        )

    def test_list_node_loaders(self):
        mock_container = MagicMock()
        mock_container.short_id = "123"
        mock_container.name = "test_container"
        mock_container.status = "running"
        mock_container.labels = {"node_label": "Person"}
        self.mock_client.containers.list.return_value = [mock_container]

        result = self.service.list_node_loaders()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {
            "id": "123",
            "name": "test_container",
            "status": "running",
            "node_label": "Person"
        })
        self.mock_client.containers.list.assert_called_once_with(
            all=True, filters={"label": "component=node-loader"}
        )

    def test_run_edge_loader_scopes_group_and_rejection_file_by_run(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                config_path="config.yaml", network="test_net", rejection_dir=rejection_dir,
                run_id="run-A", consumer_group_prefix="loader",
            )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["image"] == "graph-loader-edge:latest"
        assert call["remove"] is False
        assert call["labels"]["component"] == "edge-loader"
        assert call["labels"]["run_id"] == "run-A"
        assert call["environment"]["KAFKA_GROUP_ID"] == "loader-WORKS_AT-run-A"
        assert call["environment"]["REJECTION_LOG_PATH"].endswith("WORKS_AT-0-72756e2d41.jsonl")
        # Finite bulk (non-gated) replicas get their replica count so each
        # statically assigns p % N == replica id instead of subscribing.
        assert call["command"][-6:] == [
            "--replica-id", "0", "--run-id", "run-A", "--replica-count", "1",
        ]

    def test_run_edge_loader_publishes_metrics_port_and_labels(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                config_path="config.yaml", network="test_net", rejection_dir=rejection_dir,
            )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["labels"]["graph_loader.metrics_port"] == "9464"
        assert call["labels"]["graph_loader.kind"] == "edge"

    def test_run_edge_loader_bulk_passes_replica_count_to_every_replica(self):
        self.mock_client.containers.run.side_effect = lambda **kwargs: MagicMock()
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="bulk", replicas=3,
                config_path="config.yaml", network="test_net", rejection_dir=rejection_dir,
                run_id="run-A", consumer_group_prefix="loader",
            )
        commands = sorted(
            (call.kwargs["command"] for call in self.mock_client.containers.run.call_args_list),
            key=lambda command: command[command.index("--replica-id") + 1],
        )
        assert [command[command.index("--replica-id") + 1] for command in commands] == ["0", "1", "2"]
        assert all(command[command.index("--replica-count") + 1] == "3" for command in commands)

    def test_run_edge_loader_stream_and_slot_gated_keep_group_subscription(self):
        self.mock_client.containers.run.return_value = MagicMock()
        schema = SimpleNamespace(edges=(
            SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(
                source="Person", target="Company", is_self_referencing=False,
            )),
        ))
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="stream", replicas=2,
                config_path="config.yaml", rejection_dir=rejection_dir, run_id="run-A",
            )
            stream_command = self.mock_client.containers.run.call_args.kwargs["command"]
            with patch("src.orchestrator.docker_service.load_schema", return_value=schema):
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk", replicas=2,
                    config_path="config.yaml", rejection_dir=rejection_dir, run_id="run-A",
                    slot_gating=True, coordination_topic="clock-topic", fleet_edge_types="WORKS_AT",
                )
            gated_command = self.mock_client.containers.run.call_args.kwargs["command"]
        assert "--replica-count" not in stream_command
        assert "--replica-count" not in gated_command

    def test_write_timing_directory_reaches_node_and_edge_replicas(self):
        self.mock_client.containers.run.return_value = MagicMock()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict("os.environ", {"GRAPH_LOADER_TIMING_DIR": directory}):
                self.service.run_node_loader(
                    node_label="Person", topic="person_topic", mode="bulk",
                    config_path="config.yaml", network="test_net", rejection_dir=directory,
                )
                node_call = self.mock_client.containers.run.call_args.kwargs
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                    config_path="config.yaml", network="test_net", rejection_dir=directory,
                )
                edge_call = self.mock_client.containers.run.call_args.kwargs
        assert node_call["volumes"][directory] == {"bind": "/app/write-timings", "mode": "rw"}
        assert node_call["environment"]["GRAPH_LOADER_TIMING_LOG_PATH"] == "/app/write-timings/node-Person-0.log"
        assert edge_call["volumes"][directory] == {"bind": "/app/write-timings", "mode": "rw"}
        assert edge_call["environment"]["GRAPH_LOADER_TIMING_LOG_PATH"] == "/app/write-timings/edge-WORKS_AT-0.log"

    def test_timing_tag_keeps_precedence_substage_files_apart(self):
        self.mock_client.containers.run.return_value = MagicMock()
        seen = {}
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict("os.environ", {"GRAPH_LOADER_TIMING_DIR": directory}):
                for tag in (None, "t0", "t1"):
                    self.service.run_edge_loader(
                        edge_type="WORKS_AT", topic="works-at-events", mode="bulk", replicas=2,
                        config_path="config.yaml", network="test_net", rejection_dir=directory,
                        run_id=f"run-{tag}", timing_tag=tag,
                    )
                    env = self.mock_client.containers.run.call_args.kwargs["environment"]
                    seen[tag] = (env["GRAPH_LOADER_TIMING_LOG_PATH"], env["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"])
        assert seen[None] == ("/app/write-timings/edge-WORKS_AT-1.log", "/app/write-timings/metrics-edge-WORKS_AT-1.prom")
        assert seen["t0"] == ("/app/write-timings/edge-WORKS_AT.t0-1.log", "/app/write-timings/metrics-edge-WORKS_AT.t0-1.prom")
        assert seen["t1"][0] == "/app/write-timings/edge-WORKS_AT.t1-1.log"
        assert len({paths for paths in seen.values()}) == 3

    def test_run_edge_loader_slot_gating_scopes_distinct_clock_group(self):
        self.mock_client.containers.run.return_value = MagicMock()
        schema = SimpleNamespace(edges=(
            SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(
                source="Person", target="Company", is_self_referencing=False,
            )),
        ))
        with patch("src.orchestrator.docker_service.load_schema", return_value=schema):
            with tempfile.TemporaryDirectory() as rejection_dir:
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                    config_path="config.yaml", rejection_dir=rejection_dir, run_id="run-A",
                    consumer_group_prefix="loader", slot_gating=True, coordination_topic="clock-topic", fleet_edge_types="WORKS_AT",
                )
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["command"][-5:] == ["--slot-gating", "--coordination-topic", "clock-topic", "--fleet-edge-types", "WORKS_AT"]
        assert call["environment"]["KAFKA_GROUP_ID"] == "loader-WORKS_AT-run-A"
        assert call["environment"]["KAFKA_COORDINATION_GROUP_ID"] == "loader-WORKS_AT-run-A-clock-0"
        assert call["environment"]["KAFKA_GROUP_ID"] != call["environment"]["KAFKA_COORDINATION_GROUP_ID"]

    def test_run_edge_loader_rejects_incomplete_slot_contract_before_directory_or_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            rejection_dir = str(Path(temporary) / "not-created")
            invalid = (
                {"slot_gating": True, "coordination_topic": None, "run_id": "run-A"},
                {"slot_gating": True, "coordination_topic": " ", "run_id": "run-A"},
                {"slot_gating": False, "coordination_topic": "clock-topic", "run_id": "run-A"},
                {"slot_gating": True, "coordination_topic": "clock-topic", "run_id": None},
                {"slot_gating": True, "coordination_topic": "clock-topic", "run_id": " "},
            )
            for kwargs in invalid:
                with self.assertRaises(ValueError):
                    self.service.run_edge_loader(
                        edge_type="WORKS_AT", topic="works-at-events", mode="bulk", config_path="config.yaml",
                        rejection_dir=rejection_dir, **kwargs,
                    )
                assert not Path(rejection_dir).exists()
        self.mock_client.containers.run.assert_not_called()

    def test_run_edge_loader_rejects_forged_slot_edge_before_directory_or_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            rejection_dir = str(Path(temporary) / "not-created")
            with self.assertRaisesRegex(
                ValueError,
                r"stage=launch run_id=run-A edge=KNOWS.*absent from fleet_edge_types",
            ):
                self.service.run_edge_loader(
                    edge_type="KNOWS", topic="knows-events", mode="bulk", config_path="config.yaml",
                    rejection_dir=rejection_dir, slot_gating=True, coordination_topic="clock-topic",
                    fleet_edge_types="WORKS_AT", run_id="run-A",
                )
            assert not Path(rejection_dir).exists()
        self.mock_client.containers.run.assert_not_called()

    def test_run_edge_loader_rejects_self_reference_contract_before_directory_or_container(self):
        schema = SimpleNamespace(edges=(
            SimpleNamespace(type="KNOWS", nodes=SimpleNamespace(
                source="Person", target="Person", is_self_referencing=True,
            )),
            SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(
                source="Person", target="Company", is_self_referencing=False,
            )),
        ))
        with tempfile.TemporaryDirectory() as temporary:
            rejection_dir = str(Path(temporary) / "not-created")
            with patch("src.orchestrator.docker_service.load_schema", return_value=schema):
                with self.assertRaisesRegex(
                    ValueError,
                    r"stage=launch run_id=run-A edge=KNOWS.*every eligible schema edge",
                ):
                    self.service.run_edge_loader(
                        edge_type="KNOWS", topic="knows-events", mode="bulk", config_path="config.yaml",
                        rejection_dir=rejection_dir, slot_gating=True, coordination_topic="clock-topic",
                        fleet_edge_types="KNOWS,WORKS_AT", run_id="run-A",
                    )
            assert not Path(rejection_dir).exists()
        self.mock_client.containers.run.assert_not_called()

    def test_run_edge_loader_attributes_schema_load_failure_before_directory_or_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            rejection_dir = str(Path(temporary) / "not-created")
            with patch(
                "src.orchestrator.docker_service.load_schema",
                side_effect=SchemaLoadError("invalid schema"),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    r"stage=launch run_id=run-A edge=WORKS_AT.*invalid schema",
                ):
                    self.service.run_edge_loader(
                        edge_type="WORKS_AT", topic="works-at-events", mode="bulk", config_path="config.yaml",
                        rejection_dir=rejection_dir, slot_gating=True, coordination_topic="clock-topic",
                        fleet_edge_types="WORKS_AT", run_id="run-A",
                    )
            assert not Path(rejection_dir).exists()
        self.mock_client.containers.run.assert_not_called()

    def test_build_edge_image_uses_edge_dockerfile(self):
        self.service.build_edge_image()
        self.mock_client.images.build.assert_called_once_with(
            path=".", dockerfile="Dockerfile.edge_loader", tag="graph-loader-edge:latest", rm=True
        )

    def test_run_global_clock_is_exact_and_run_scoped(self):
        self.mock_client.containers.run.return_value = MagicMock()
        clock = self.service.run_global_batch_clock(
            run_id="run-A", config_path="config.yaml", fleet_edge_types="BOUGHT,WORKS_AT",
            coordination_topic="graph.loader.coordination",
        )
        assert clock is self.mock_client.containers.run.return_value
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["name"] == "graph-loader-clock-72756e2d41"
        assert call["labels"] == {
            "app": "graph-loader", "component": "global-batch-clock", "run_id": "run-A",
            "graph_loader.metrics_port": "9464", "graph_loader.kind": "clock",
            "graph_loader.metrics_enabled": "0",
        }
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert "--fleet-edge-types" in call["command"]

    def test_edge_partial_launch_rolls_back_only_started_containers(self):
        started = MagicMock()
        self.mock_client.containers.run.side_effect = [started, RuntimeError("launch failed")]
        with tempfile.TemporaryDirectory() as rejection_dir:
            with self.assertRaisesRegex(RuntimeError, "launch failed"):
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                    config_path="config.yaml", replicas=2, rejection_dir=rejection_dir,
                    run_id="run-A",
                )
        started.stop.assert_called_once()


class TestConcurrentReplicaLaunch(unittest.TestCase):
    """Replica launches overlap but keep order, names, and rollback exact."""

    def _barrier_client(self, replicas, fail_replica=None):
        import threading

        barrier = threading.Barrier(replicas, timeout=5)
        started = {}
        lock = threading.Lock()

        def run(**kwargs):
            replica_id = kwargs["labels"]["replica_id"]
            # Every replica must be in flight at once, or this times out.
            barrier.wait()
            if replica_id == fail_replica:
                raise RuntimeError(f"launch failed replica={replica_id}")
            container = MagicMock(name=kwargs["name"])
            with lock:
                started[replica_id] = container
            return container

        client = MagicMock()
        client.containers.run.side_effect = run
        return client, started

    def test_node_replicas_launch_concurrently_in_replica_order(self):
        client, started = self._barrier_client(4)
        with tempfile.TemporaryDirectory() as rejection_dir:
            containers = DockerService(client=client).run_node_loader(
                node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                replicas=4, rejection_dir=rejection_dir, run_id="run-A",
            )
        self.assertEqual(containers, [started[str(index)] for index in range(4)])
        names = sorted(call.kwargs["name"] for call in client.containers.run.call_args_list)
        self.assertEqual(
            names, [f"graph-loader-node-Person-{index}-{'run-A'.encode().hex()}" for index in range(4)]
        )
        for call in client.containers.run.call_args_list:
            # Bulk replicas are kept after exit for diagnosis; the CLI removes them.
            self.assertIs(call.kwargs["remove"], False)
            replica_id = call.kwargs["labels"]["replica_id"]
            self.assertEqual(
                call.kwargs["environment"]["REJECTION_LOG_PATH"],
                f"/app/rejections/Person-{replica_id}.jsonl",
            )

    def test_node_partial_launch_stops_every_started_replica(self):
        client, started = self._barrier_client(3, fail_replica="1")
        with tempfile.TemporaryDirectory() as rejection_dir:
            with self.assertRaisesRegex(RuntimeError, "replica=1"):
                DockerService(client=client).run_node_loader(
                    node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                    replicas=3, rejection_dir=rejection_dir,
                )
        self.assertEqual(set(started), {"0", "2"})
        for container in started.values():
            container.stop.assert_called_once()
            container.remove.assert_called_once_with(force=True)  # bulk: no auto-remove

    def test_edge_replicas_launch_concurrently_and_roll_back_with_removal(self):
        client, started = self._barrier_client(3, fail_replica="2")
        with tempfile.TemporaryDirectory() as rejection_dir:
            with self.assertRaisesRegex(RuntimeError, "replica=2"):
                DockerService(client=client).run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode="bulk",
                    config_path="config.yaml", replicas=3, rejection_dir=rejection_dir, run_id="run-A",
                )
        self.assertEqual(set(started), {"0", "1"})
        for container in started.values():
            container.stop.assert_called_once()
            container.remove.assert_called_once_with(force=True)

    def test_invalid_run_id_fails_before_any_launch(self):
        client = MagicMock()
        with tempfile.TemporaryDirectory() as rejection_dir:
            with self.assertRaises(ValueError):
                DockerService(client=client).run_node_loader(
                    node_label="Person", topic="person_topic", mode="bulk", config_path="config.yaml",
                    replicas=2, rejection_dir=rejection_dir, run_id="",
                )
        client.containers.run.assert_not_called()


class TestBucketLeaseWiring(unittest.TestCase):
    """The lease coordinator container and edge-loader lease environment."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.service = DockerService(client=self.mock_client)

    def test_run_bucket_lease_server_reuses_edge_image_with_lease_entrypoint(self):
        with patch.dict("os.environ", {"LOADER_CPUSET": "12-15"}):
            container = self.service.run_bucket_lease_server(
                run_id="run-A", bucket_count=16, network="loader-net", keyed_labels=["Region", "Genre"],
            )
        assert container is self.mock_client.containers.run.return_value
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["image"] == "graph-loader-edge:latest"
        assert call["entrypoint"] == ["python", "-m", "src.orchestrator.lease_server"]
        assert call["command"] == [
            "--host", "0.0.0.0", "--port", "7700", "--buckets", "16", "--scheme", "Genre,Region",
        ]
        from src.orchestrator.docker_service import _dns_safe_run_token
        assert call["name"] == f"graph-loader-lease-{_dns_safe_run_token('run-A')}"
        # Story 10 E2E finding: this name is DNS-resolved by other
        # containers (bucket_lease_address(prefer_name=True)), so it must
        # never depend on run_id's length -- unlike node/edge/clock
        # container names, whose own name nobody ever resolves.
        assert len(call["name"]) <= 63
        assert call["network"] == "loader-net" and call["remove"] is False and call["detach"] is True
        assert call["labels"] == {
            "app": "graph-loader", "component": "bucket-lease-server", "run_id": "run-A",
            "graph_loader.metrics_port": "9464", "graph_loader.kind": "lease-server",
            "graph_loader.metrics_enabled": "0",
        }
        assert call["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert call["cpuset_cpus"] == "12-15"

    def test_run_bucket_lease_server_name_stays_dns_safe_for_a_uuid_run_id(self):
        """Story 10 E2E finding: a real ``uuid.uuid4().hex`` run id (the
        CLI's own default when ``--run-id`` is not given) is 32 ASCII
        characters; ``.encode('utf-8').hex()`` doubles that to 64, and
        ``graph-loader-lease-`` plus 64 more characters is a single DNS
        label over the 63-octet limit. Resolving such a name from inside
        a container raises ``UnicodeError: label too long`` -- confirmed
        against a real Docker network -- silently breaking every default
        (no ``--run-id``) streaming lease run. The container's name must
        stay short regardless of run_id's length or content.
        """
        import uuid
        run_id = uuid.uuid4().hex
        self.service.run_bucket_lease_server(run_id=run_id, bucket_count=16, network="loader-net")
        call = self.mock_client.containers.run.call_args.kwargs
        assert len(call["name"]) <= 63
        assert call["labels"]["run_id"] == run_id

    def test_run_bucket_lease_server_validates_inputs(self):
        for kwargs in ({"run_id": "", "bucket_count": 16}, {"run_id": "r", "bucket_count": 0},
                       {"run_id": "r", "bucket_count": 16, "port": 0},
                       {"run_id": "r", "bucket_count": 16, "keyed_labels": ["bad label"]}):
            with self.assertRaises(ValueError):
                self.service.run_bucket_lease_server(**kwargs)
        self.mock_client.containers.run.assert_not_called()

    def test_bucket_lease_address_prefers_network_ip(self):
        container = MagicMock()
        container.name = "graph-loader-lease-x"
        container.attrs = {"NetworkSettings": {"Networks": {"bridge": {"IPAddress": "172.17.0.9"}}}}
        assert DockerService.bucket_lease_address(container, network="bridge") == "172.17.0.9:7700"
        container.reload.assert_called_once()
        container.attrs = {"NetworkSettings": {"Networks": {}}}
        assert DockerService.bucket_lease_address(container, network="loader-net") == "graph-loader-lease-x:7700"

    def test_bucket_lease_address_prefer_name_skips_ip_lookup_entirely(self):
        """P5-S1 Story 5: a restarted streaming lease server is a new
        container with a new IP -- prefer_name=True must return the
        stable name without even calling reload()/inspecting IPs, relying
        on the run's own network's embedded DNS."""
        container = MagicMock()
        container.name = "graph-loader-lease-x"
        assert DockerService.bucket_lease_address(
            container, network="graph-loader-net", prefer_name=True,
        ) == "graph-loader-lease-x:7700"
        container.reload.assert_not_called()

    def test_edge_loader_lease_environment_passthrough(self):
        with tempfile.TemporaryDirectory() as rejection_dir, \
                patch.dict("os.environ", {"GRAPH_LOADER_EDGE_EXPLICIT_LOCKS": "0"}):
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="bulk", config_path="config.yaml",
                replicas=2, rejection_dir=rejection_dir, run_id="run-A",
                lease_address="172.17.0.9:7700", lease_buckets=8, outer_batch_size=32000,
                lease_keyed_labels=("Region", "Genre"),
            )
        assert self.mock_client.containers.run.call_count == 2
        for call in self.mock_client.containers.run.call_args_list:
            env = call.kwargs["environment"]
            # P5-S1 Story 1: both the current and the deprecated env var
            # name are set to the same value for one release.
            assert env["GRAPH_LOADER_BUCKET_LEASE_ADDR"] == "172.17.0.9:7700"
            assert env["GRAPH_LOADER_LEASE_ADDR"] == "172.17.0.9:7700"
            assert env["GRAPH_LOADER_LEASE_BUCKETS"] == "8"
            assert env["GRAPH_LOADER_LEASE_KEYED_LABELS"] == "Genre,Region"
            assert env["GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE"] == "32000"
            assert env["GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"] == "0"

    def test_edge_loader_forwards_lease_merge_rows_to_stream_only(self):
        from src.loader.edge_execution import LEASE_MERGE_ROWS_ENV
        from src.orchestrator import docker_service as docker_service_module

        # The launcher's constant is the name the container's lease coordinator reads.
        assert docker_service_module.LEASE_MERGE_ROWS_ENV == LEASE_MERGE_ROWS_ENV == "GRAPH_LOADER_LEASE_MERGE_ROWS"
        seen = {}
        for mode in ("bulk", "stream"):
            self.mock_client.containers.run.reset_mock()
            with tempfile.TemporaryDirectory() as rejection_dir, \
                    patch.dict("os.environ", {"GRAPH_LOADER_LEASE_MERGE_ROWS": "64"}):
                self.service.run_edge_loader(
                    edge_type="WORKS_AT", topic="works-at-events", mode=mode, config_path="config.yaml",
                    rejection_dir=rejection_dir, run_id="run-A", lease_address="172.17.0.9:7700", lease_buckets=8,
                )
            seen[mode] = self.mock_client.containers.run.call_args.kwargs["environment"].get("GRAPH_LOADER_LEASE_MERGE_ROWS")
        # Bulk (--edge-leases) edge containers never get it: nothing may change the bulk path.
        assert seen == {"bulk": None, "stream": "64"}

    def test_edge_loader_without_leases_sets_no_lease_environment(self):
        with tempfile.TemporaryDirectory() as rejection_dir:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="works-at-events", mode="bulk", config_path="config.yaml",
                rejection_dir=rejection_dir, run_id="run-A",
            )
        env = self.mock_client.containers.run.call_args.kwargs["environment"]
        assert not any(key.startswith("GRAPH_LOADER_LEASE") for key in env)
        assert "GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE" not in env

    def test_edge_loader_rejects_inconsistent_lease_arguments(self):
        cases = (
            {"lease_address": "no-port"},
            {"lease_buckets": 8},
            {"lease_keyed_labels": ("Genre",)},
            {"lease_address": "lease:7700", "lease_buckets": 0},
            {"lease_address": "lease:7700", "outer_batch_size": 0},
            {"lease_address": "lease:7700", "slot_gating": True, "coordination_topic": "c",
             "fleet_edge_types": "WORKS_AT"},
        )
        with tempfile.TemporaryDirectory() as rejection_dir:
            for kwargs in cases:
                with self.assertRaises(ValueError, msg=kwargs):
                    self.service.run_edge_loader(
                        edge_type="WORKS_AT", topic="works-at-events", mode="bulk", config_path="config.yaml",
                        rejection_dir=rejection_dir, run_id="run-A", **kwargs,
                    )
        self.mock_client.containers.run.assert_not_called()


class TestRetryConsumerWiring(unittest.TestCase):
    """P5-S4 Story 6: DockerService.run_retry_consumer."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.service = DockerService(client=self.mock_client)

    def test_run_retry_consumer_launches_one_replica_from_the_edge_image(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            containers = self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq", "knows-events.dlq"], network="loader-net",
            )
        assert containers == [self.mock_client.containers.run.return_value]
        call = self.mock_client.containers.run.call_args.kwargs
        assert call["image"] == "graph-loader-edge:latest"
        assert call["entrypoint"] == ["python", "-m", "src.loader.retry_consumer"]
        assert call["command"] == [
            "--config", "/app/runtime_schema.yaml", "--run-id", "run-A",
            "--dlq-topics", "works-at-events.dlq,knows-events.dlq",
        ]
        assert call["network"] == "loader-net" and call["detach"] is True and call["remove"] is False
        assert call["labels"]["component"] == "retry-consumer"
        assert call["labels"]["run_id"] == "run-A"
        assert call["labels"]["replica_id"] == "0"
        env = call["environment"]
        assert env["GRAPH_LOADER_BUCKET_LEASE_ADDR"] == "172.17.0.9:7700"
        assert env["GRAPH_LOADER_LEASE_ADDR"] == "172.17.0.9:7700"
        assert env["GRAPH_LOADER_RUN_ID"] == "run-A"
        # Stable, prefix-namespaced: a restart resumes the DLQ (stable stream groups).
        assert env["KAFKA_GROUP_ID"] == "graph-loader-retry-consumer"

    def test_run_retry_consumer_passes_through_lease_buckets_and_keyed_labels(self):
        """Must match the run's live edge-loader fleet exactly, so this
        replica's own BucketCellRouter maps nodes to lease resources
        identically."""
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq"], lease_buckets=8, lease_keyed_labels=("Region", "Genre"),
            )
        env = self.mock_client.containers.run.call_args.kwargs["environment"]
        assert env["GRAPH_LOADER_LEASE_BUCKETS"] == "8"
        assert env["GRAPH_LOADER_LEASE_KEYED_LABELS"] == "Genre,Region"

    def test_run_retry_consumer_launches_n_replicas_in_one_group(self):
        self.mock_client.containers.run.side_effect = [MagicMock(), MagicMock(), MagicMock()]
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            containers = self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq"], replicas=3,
            )
        assert len(containers) == 3
        assert self.mock_client.containers.run.call_count == 3
        replica_ids = {call.kwargs["labels"]["replica_id"] for call in self.mock_client.containers.run.call_args_list}
        assert replica_ids == {"0", "1", "2"}
        # Every replica joins the SAME consumer group -- one fleet-wide
        # consumer group, not one per replica.
        group_ids = {call.kwargs["environment"]["KAFKA_GROUP_ID"] for call in self.mock_client.containers.run.call_args_list}
        assert group_ids == {"graph-loader-retry-consumer"}

    def test_run_retry_consumer_group_is_stable_and_namespaced_by_the_prefix(self):
        groups = []
        for run_id, prefix in (("run-A", "deploy-one"), ("run-B", "deploy-one"), ("run-C", "deploy-two")):
            with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
                self.service.run_retry_consumer(
                    run_id=run_id, config_path=config_file.name, lease_address="172.17.0.9:7700",
                    dlq_topics=["works-at-events.dlq"], consumer_group_prefix=prefix,
                )
            groups.append(self.mock_client.containers.run.call_args.kwargs["environment"]["KAFKA_GROUP_ID"])
        assert groups == ["deploy-one-retry-consumer", "deploy-one-retry-consumer", "deploy-two-retry-consumer"]

    def test_run_retry_consumer_forwards_lease_merge_rows(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file, \
                patch.dict("os.environ", {"GRAPH_LOADER_LEASE_MERGE_ROWS": "64"}):
            self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq"],
            )
        assert self.mock_client.containers.run.call_args.kwargs["environment"]["GRAPH_LOADER_LEASE_MERGE_ROWS"] == "64"

    def test_run_retry_consumer_omits_lease_merge_rows_when_unset_or_overridden(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file, \
                patch.dict("os.environ", {}, clear=False):
            os.environ.pop("GRAPH_LOADER_LEASE_MERGE_ROWS", None)
            self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq"],
            )
            assert "GRAPH_LOADER_LEASE_MERGE_ROWS" not in self.mock_client.containers.run.call_args.kwargs["environment"]
            os.environ["GRAPH_LOADER_LEASE_MERGE_ROWS"] = "64"
            self.service.run_retry_consumer(
                run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                dlq_topics=["works-at-events.dlq"], environment={"NEO4J_URI": "bolt://x:7687"},
            )
            assert "GRAPH_LOADER_LEASE_MERGE_ROWS" not in self.mock_client.containers.run.call_args.kwargs["environment"]

    def test_run_retry_consumer_requires_at_least_one_dlq_topic(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            with self.assertRaises(ValueError):
                self.service.run_retry_consumer(
                    run_id="run-A", config_path=config_file.name, lease_address="172.17.0.9:7700",
                    dlq_topics=[],
                )
        self.mock_client.containers.run.assert_not_called()

    def test_run_retry_consumer_rejects_a_malformed_lease_address(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            with self.assertRaises(ValueError):
                self.service.run_retry_consumer(
                    run_id="run-A", config_path=config_file.name, lease_address="no-port",
                    dlq_topics=["works-at-events.dlq"],
                )
        self.mock_client.containers.run.assert_not_called()

    def test_run_retry_consumer_rejects_blank_run_id(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as config_file:
            with self.assertRaises(ValueError):
                self.service.run_retry_consumer(
                    run_id="", config_path=config_file.name, lease_address="172.17.0.9:7700",
                    dlq_topics=["works-at-events.dlq"],
                )
        self.mock_client.containers.run.assert_not_called()


class TestInitProcessOnEveryLaunch(unittest.TestCase):
    """D1: PID 1 Python ignores an unhandled SIGTERM while starting, so every
    long-lived container runs under the Docker init (tini)."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.service = DockerService(client=self.mock_client)

    def _all_calls_use_init(self):
        calls = self.mock_client.containers.run.call_args_list
        assert calls
        for call in calls:
            assert call.kwargs.get("init") is True

    def test_node_loader_bulk_and_stream(self):
        with tempfile.TemporaryDirectory() as rej:
            for mode in ("bulk", "stream"):
                self.service.run_node_loader(
                    node_label="Person", topic="t", mode=mode, config_path="c.yaml", rejection_dir=rej,
                )
        self._all_calls_use_init()

    def test_edge_loader(self):
        with tempfile.TemporaryDirectory() as rej:
            self.service.run_edge_loader(
                edge_type="WORKS_AT", topic="t", mode="stream", config_path="c.yaml", rejection_dir=rej,
            )
        self._all_calls_use_init()

    def test_lease_server_clock_retry_dispatcher_writers(self):
        self.service.run_bucket_lease_server(run_id="r", bucket_count=4, network="n")
        self.service.run_global_batch_clock(
            run_id="r", config_path="c.yaml", fleet_edge_types="WORKS_AT", coordination_topic="coord",
        )
        with tempfile.NamedTemporaryFile(suffix=".yaml") as cfg, tempfile.NamedTemporaryFile() as plan:
            self.service.run_retry_consumer(
                run_id="r", config_path=cfg.name, lease_address="1.2.3.4:7700", dlq_topics=["a.dlq"],
            )
            self.service.run_planned_dispatcher(run_id="r", plan_path=plan.name)
            self.service.run_planned_edge_writers(
                run_id="r", plan_path=plan.name, config_path=cfg.name,
                dispatcher_address="1.2.3.4:7800", writers=2,
            )
        self.assertEqual(self.mock_client.containers.run.call_count, 6)
        self._all_calls_use_init()
