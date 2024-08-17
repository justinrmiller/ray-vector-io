import time

import ray
from qdrant import qdrant_datasource
from qdrant_client import QdrantClient
from ray.data import read_datasource

ray.init()

HOST = "http://localhost"
PORT = 6334
COLLECTION_NAME = "collection"
LIMIT = 100_000

qdrant_client = QdrantClient(host=HOST, port=6334, prefer_grpc=True)

qdrant_datasource = qdrant_datasource.QdrantDatasource(
    client=qdrant_client,
    collection_name=COLLECTION_NAME,
    with_payload=True,
    with_vectors=True,
    limit=LIMIT,
)

dataset = read_datasource(qdrant_datasource, override_num_blocks=3)

start_time = time.perf_counter()

output_path_parquet = "local://parquet/output"
dataset.write_parquet(output_path_parquet)

end_time = time.perf_counter()

print(
    f"Time taken to write parquet files to {output_path_parquet}: "
    f"{end_time - start_time:0.2f} seconds"
)

# output_path_csv = "local://csv/output"
# dataset.write_csv(output_path_csv)

ray.shutdown()
