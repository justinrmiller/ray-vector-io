import logging
from typing import TYPE_CHECKING, Dict, List, Any, Iterable, Optional

from ray.data.block import Block
from ray.data.datasource.datasink import Datasink
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import BlockAccessor
from ray.util.annotations import PublicAPI

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

logger = logging.getLogger(__name__)

class QdrantDatasink(Datasink):
    def __init__(self, host: str, port: int, collection_name: str, batch_size: int = 100) -> None:
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.batch_size = batch_size
        self._client = None

    def _get_or_create_client(self):
        if self._client is None:
            self._client = QdrantClient(host=self.host, port=self.port)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        self._get_or_create_client()
        self._validate_collection_exists(self.collection_name)

        def write_block(client, collection_name: str, block: Block, batch_size: int):
            block = BlockAccessor.for_block(block).to_pandas()
            points = [
                PointStruct(
                    id=row['id'],
                    payload=row['payload'],
                    vector=row['vector']
                )
                for _, row in block.iterrows()
            ]
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                client.upsert(collection_name, batch)

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()

        write_block(self._client, self.collection_name, block, self.batch_size)

        return "ok"

    def _validate_collection_exists(self, collection_name: str):
        collections = self._client.get_collections().collections
        if collection_name not in [collection.name for collection in collections]:
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