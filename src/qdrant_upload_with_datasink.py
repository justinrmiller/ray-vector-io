# Example usage in a script
import ray
import time

from qdrant_client import QdrantClient

from qdrant import qdrant_datasink

ray.init()

HOST = "localhost"
PORT = 6334
COLLECTION_NAME = "collection"
BATCH_SIZE = 200
PARQUET_FILE_PATH = "parquet/output"

dataset = ray.data.read_parquet(PARQUET_FILE_PATH)

# Optionally filter the dataset or perform any transformation
# filtered_dataset = dataset.filter(...)

qdrant_client = QdrantClient(
    host=HOST,
    port=6334,
    prefer_grpc=True
)

qdrant_datasink = qdrant_datasink.QdrantDatasink(
    client=qdrant_client,
    collection_name=COLLECTION_NAME,
    batch_size=BATCH_SIZE,
)

start_time = time.perf_counter()

dataset.write_datasink(qdrant_datasink)

end_time = time.perf_counter()

print(f"Time taken to write {dataset.count()} rows: {end_time - start_time:0.2f} seconds")

ray.shutdown()
