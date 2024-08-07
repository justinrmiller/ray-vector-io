import pyarrow as pa

from typing import List, Optional

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

from qdrant_client import QdrantClient, models, grpc


@PublicAPI(stability="alpha")
class QdrantDatasource(Datasource):
    """Datasource for reading from Qdrant."""

    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
        limit: int = 1000,
        with_payload: bool = True,
        with_vectors: bool = False,
        scroll_size: int = 256,
        **scroll_kwargs,
    ):
        self._collection_name = collection_name
        self._limit = limit
        self._with_payload = with_payload
        self._with_vectors = with_vectors
        self._scroll_size = scroll_size
        self._scroll_kwargs = scroll_kwargs

        # Can't set QdrantClient directly because
        # Ray.io raises serialization errors
        self._client_opts = client.init_options

    @property
    def _client(self) -> QdrantClient:
        return QdrantClient(**self._client_opts)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def make_block() -> Block:
            print("Making block")
            points = self._paginated_scroll()

            p = {"id": pa.array([item.id for item in points])}

            payloads_non_empty = any(bool(d.payload) for d in points)
            if payloads_non_empty:
                p["payload"] = pa.array([item.payload for item in points])

            vectors_non_empty = any(bool(d.vector) for d in points)
            if vectors_non_empty:
                p["vector"] = pa.array([item.vector for item in points])

            points_pyarrow = pa.table(p)

            return points_pyarrow

        metadata = BlockMetadata(
            num_rows=self._limit,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )

        return [
            ReadTask(
                lambda args = (): [make_block(*args)],
                metadata,
            )
        ]

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _validate_collection_exists(self, collection_name: str):
        if not self._client.collection_exists(collection_name):
            raise ValueError(f"Collection {collection_name} does not exist.")

    def _paginated_scroll(self) -> List[models.Record]:
        self._validate_collection_exists(self._collection_name)

        results: List[models.Record] = []
        next_offset = None
        stop_scrolling = False

        remaining_limit = self._limit
        while not stop_scrolling:
            scroll_limit = min(self._scroll_size, remaining_limit)
            records, next_offset = self._client.scroll(
                self._collection_name,
                limit=scroll_limit,
                offset=next_offset,
                with_payload=self._with_payload,
                with_vectors=self._with_vectors,
                **self._scroll_kwargs,
            )

            results.extend(records)
            remaining_limit -= len(records)

            stop_scrolling = (
                next_offset is None
                # If `prefer_grpc`` is set to true in the client, next_offset will be a grpc.PointId object
                or (
                    isinstance(next_offset, grpc.PointId)
                    and next_offset.num == 0
                    and next_offset.uuid == ""
                )
                or remaining_limit <= 0
            )

        return results
