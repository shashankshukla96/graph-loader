import unittest
import tempfile
from unittest.mock import MagicMock, patch
from src.orchestrator.docker_service import DockerService

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
        assert call["labels"]["component"] == "edge-loader"
        assert call["labels"]["run_id"] == "run-A"
        assert call["environment"]["KAFKA_GROUP_ID"] == "loader-WORKS_AT-run-A"
        assert call["environment"]["REJECTION_LOG_PATH"].endswith("WORKS_AT-0-72756e2d41.jsonl")
        assert call["command"][-4:] == ["--replica-id", "0", "--run-id", "run-A"]

    def test_build_edge_image_uses_edge_dockerfile(self):
        self.service.build_edge_image()
        self.mock_client.images.build.assert_called_once_with(
            path=".", dockerfile="Dockerfile.edge_loader", tag="graph-loader-edge:latest", rm=True
        )

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
