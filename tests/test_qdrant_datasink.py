from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from qdrant_client import QdrantClient
from ray.data.block import Block

from ray_vector_io.qdrant.qdrant_datasink import QdrantDatasink


@pytest.fixture
def mock_qdrant_client():
    client = MagicMock(spec=QdrantClient)
    client.init_options = {"host": "localhost", "port": 6333}

    # Mocking the collection_exists method
    def mock_collection_exists(collection_name):
        return collection_name == "test_collection"

    client.collection_exists = MagicMock(side_effect=mock_collection_exists)

    # Mocking the creation of a collection
    client.create_collection = MagicMock()

    return client


@pytest.fixture
def mock_block():
    # Create a mock block accessor
    dict = {
        "id": [1, 2],
        "payload": [{"key": "value"}, {"key": "value2"}],
        "vector": [[0.1, 0.2], [0.2, 0.3]],
    }
    block = Block.from_pandas(pd.DataFrame.from_dict(dict))

    # Mock the BlockAccessor.for_block() to return the mock block accessor
    with patch("ray.data.block.BlockAccessor.for_block", return_value=block):
        yield block


@pytest.fixture
def qdrant_datasink(mock_qdrant_client):
    return QdrantDatasink(
        client=mock_qdrant_client,
        collection_name="test_collection",
        batch_size=2,
        parallel=1,
    )


# todo: fix this test
# def test_write_success(qdrant_datasink, mock_block):
#     ctx = TaskContext(task_idx=0)
#     qdrant_datasink._client.collection_exists = MagicMock(return_value=True)
#     qdrant_datasink._validate_collection_exists = MagicMock(return_value=True)
#
#     result = qdrant_datasink.write([mock_block], ctx)
#
#     qdrant_datasink._client.upload_points.assert_called_once()
#     assert result == "ok"
#
# todo: fix this test
# def test_write_collection_does_not_exist(qdrant_datasink, mock_block):
#     ctx = TaskContext(task_idx=0)
#     qdrant_datasink._client.collection_exists = MagicMock(return_value=False)
#
#     with pytest.raises(ValueError, match="Collection test_collection does not exist."):
#         qdrant_datasink.write([mock_block], ctx)


# todo: fix this test
# def test_validate_collection_exists(qdrant_datasink):
#     # Mock the method to return True
#     qdrant_datasink._client.collection_exists = MagicMock(return_value=True)
#     qdrant_datasink._validate_collection_exists("test_collection")
#     qdrant_datasink._client.collection_exists.assert_called_once_with("test_collection")


def test_validate_collection_not_exists(qdrant_datasink):
    # Mock the method to return False
    qdrant_datasink._client.collection_exists = MagicMock(return_value=False)
    with pytest.raises(ValueError):
        qdrant_datasink._validate_collection_exists("test_collection")


def test_supports_distributed_writes(qdrant_datasink):
    assert qdrant_datasink.supports_distributed_writes is False


def test_num_rows_per_write(qdrant_datasink):
    assert qdrant_datasink.num_rows_per_write is None
