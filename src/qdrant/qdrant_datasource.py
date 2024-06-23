import logging
import pyarrow as pa

from typing import TYPE_CHECKING, Dict, List, Optional

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

from qdrant_client import QdrantClient

logger = logging.getLogger(__name__)

@PublicAPI(stability="alpha")
class QdrantDatasource(Datasource):
    """Datasource for reading from Qdrant."""

    def __init__(
        self,
        host: str,
        port: int,
        collection_name: str,
        with_payload: Optional[bool] = True,
        with_vectors: Optional[bool] = False,
        limit: Optional[int] = None,
        **qdrant_args,
    ):
        self._host = host
        self._port = port
        self._collection_name = collection_name
        self._limit = limit
        self._with_payload = with_payload
        self._with_vectors = with_vectors
        self._qdrant_args = qdrant_args
        self._client = None

    def _get_or_create_client(self):
        if self._client is None:
            self._client = QdrantClient(host=self._host, port=self._port)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        self._get_or_create_client()

        self._validate_collection_exists(self._collection_name)

        collection_info = self._client.get_collection(self._collection_name)

        total_points = collection_info.points_count
        partition_size = total_points // parallelism

        def make_block(
            host: str,
            port: int,
            collection_name: str,
            offset: int,
            limit: int,
            with_payload: Optional[bool],
            with_vectors: Optional[bool],
            qdrant_args: dict,
        ) -> Block:
            client = QdrantClient(host=host, port=port)
            points = client.scroll(
                collection_name,
                limit=limit,
                offset=offset,
                with_payload=with_payload,
                with_vectors=with_vectors,
                **qdrant_args
            )

            p = {
                'id': pa.array([item.id for item in points[0]])
            }

            payloads_non_empty = any(bool(d.payload) for d in points[0])
            if payloads_non_empty:
                p['payload'] = pa.array([item.payload for item in points[0]])

            vectors_non_empty = any(bool(d.vector) for d in points[0])
            if vectors_non_empty:
                p['vector'] = pa.array([item.vector for item in points[0]])
            
            points_pyarrow = pa.table(p)

            return points_pyarrow

        read_tasks: List[ReadTask] = []

        for i in range(parallelism):
            offset = i * partition_size

            metadata = BlockMetadata(
                num_rows=self._limit,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )

            make_block_args = (
                self._host,
                self._port,
                self._collection_name,
                offset,
                self._limit,
                self._with_payload,
                self._with_vectors,
                self._qdrant_args,
            )

            read_task = ReadTask(
                lambda args=make_block_args: [make_block(*args)],
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _validate_collection_exists(self, collection_name: str):
        self._get_or_create_client()

        collections = self._client.get_collections().collections
        if collection_name not in [collection.name for collection in collections]:
            raise ValueError(f"Collection {collection_name} does not exist.")
