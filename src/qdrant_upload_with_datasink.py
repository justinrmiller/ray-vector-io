# Example usage in a script
import ray
from qdrant_client import QdrantClient

from qdrant import qdrant_datasink

ray.init()

HOST = "localhost"
PORT = 6333
COLLECTION_NAME = "collection"
BATCH_SIZE = 200
PARQUET_FILE_PATH = "parquet/output.parquet"

dataset = ray.data.read_parquet(PARQUET_FILE_PATH)

# Optionally filter the dataset or perform any transformation
# filtered_dataset = dataset.filter(...)

qdrant_client = QdrantClient(
    url=f"{HOST}:{PORT}"
)

qdrant_datasink = qdrant_datasink.QdrantDatasink(
    client=qdrant_client,
    collection_name=COLLECTION_NAME,
    batch_size=BATCH_SIZE,
)

dataset.write_datasink(qdrant_datasink)

ray.shutdown()
