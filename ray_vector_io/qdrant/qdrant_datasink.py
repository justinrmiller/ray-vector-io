from typing import Any, Iterable, Optional

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink


class QdrantDatasink(Datasink):
    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
        batch_size: int = 64,
        parallel: int = 1,
        **kwargs: Any,
    ) -> None:
        self._collection_name = collection_name
        self._batch_size = batch_size
        self._parallel = parallel
        self._kwargs = kwargs

        # Can't set QdrantClient directly because
        # Ray.io raises serialization errors
        self._client_opts = client.init_options

    @property
    def _client(self) -> QdrantClient:
        return QdrantClient(**self._client_opts)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        self._validate_collection_exists(self._collection_name)

        def write_block(
            client, collection_name: str, block: Block, batch_size: int, parallel: int
        ):
            block = BlockAccessor.for_block(block).to_pandas()
            points = [
                PointStruct(id=row["id"], payload=row["payload"], vector=row["vector"])
                for _, row in block.iterrows()
            ]
            client.upload_points(
                collection_name,
                points=points,
                batch_size=batch_size,
                parallel=parallel,
                **self._kwargs,
            )

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()

        write_block(
            self._client, self._collection_name, block, self._batch_size, self._parallel
        )

        return "ok"

    def _validate_collection_exists(self, collection_name: str):
        if not self._client.collection_exists(collection_name):
            raise ValueError(f"Collection {collection_name} does not exist.")

    @property
    def supports_distributed_writes(self) -> bool:
        """If ``False``, only launch write tasks on the driver's node."""

        # I think this is safer
        return False

    @property
    def num_rows_per_write(self) -> Optional[int]:
        """The target number of rows to pass to each :meth:`~ray.data.Datasink.write` call.

        If ``None``, Ray Data passes a system-chosen number of rows.
        """

        # this may be a better way of controlling max batch size?
        return None
