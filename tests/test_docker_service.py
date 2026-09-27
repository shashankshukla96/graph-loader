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
        
        call_kwargs = self.mock_client.containers.run.call_args[1]
        self.assertEqual(call_kwargs["image"], "graph-loader-node:latest")
        self.assertTrue(call_kwargs["name"].startswith("graph-loader-node-Person-"))
        self.assertEqual(call_kwargs["command"], ["--config", "/app/runtime_schema.yaml", "--node-label", "Person", "--mode", "stream", "--topic", "person_topic", "--replica-id", "1"])
        self.assertEqual(call_kwargs["network"], "test_net")
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
        calls = self.mock_client.containers.run.call_args_list
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
        assert call["command"][-4:] == ["--replica-id", "0", "--run-id", "run-A"]

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
        assert call["labels"] == {"app": "graph-loader", "component": "global-batch-clock", "run_id": "run-A"}
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
