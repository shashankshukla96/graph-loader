import unittest
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

        containers = self.service.run_node_loader(
            node_label="Person",
            topic="person_topic",
            mode="stream",
            config_path="config.yaml",
            replicas=2,
            network="test_net"
        )

        self.assertEqual(len(containers), 2)
        self.assertEqual(self.mock_client.containers.run.call_count, 2)
        
        call_kwargs = self.mock_client.containers.run.call_args[1]
        self.assertEqual(call_kwargs["image"], "graph-loader-node:latest")
        self.assertTrue(call_kwargs["name"].startswith("graph-loader-node-Person-"))
        self.assertEqual(call_kwargs["command"], ["--config", "/app/runtime_schema.yaml", "--node-label", "Person", "--mode", "stream", "--topic", "person_topic"])
        self.assertEqual(call_kwargs["network"], "test_net")
        self.assertIn("NEO4J_URI", call_kwargs["environment"])
        self.assertEqual(call_kwargs["labels"]["node_label"], "Person")
        self.assertIn("volumes", call_kwargs)
        # Check volume is bound correctly (ignoring the exact host path)
        volumes = call_kwargs["volumes"]
        self.assertEqual(len(volumes), 1)
        mount = list(volumes.values())[0]
        self.assertEqual(mount["bind"], "/app/runtime_schema.yaml")
        self.assertEqual(mount["mode"], "ro")

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
