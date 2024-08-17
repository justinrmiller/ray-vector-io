import ray
from qdrant_client import QdrantClient
from ray.data import read_datasource

from qdrant import qdrant_datasource

ray.init()

HOST = "http://localhost"
PORT = 6333
COLLECTION_NAME = "collection"

qdrant_client = QdrantClient(
    url=f"{HOST}:{PORT}"
)

qdrant_datasource = qdrant_datasource.QdrantDatasource(
    client=qdrant_client,
    collection_name=COLLECTION_NAME,
    with_payload=True,
    with_vectors=True,
    limit=10_000,
)

dataset = read_datasource(
    qdrant_datasource,
    override_num_blocks=1
)

output_path_parquet = "local://parquet/output.parquet"
dataset.write_parquet(output_path_parquet)

output_path_csv = "local://csv/output.csv"
dataset.write_csv(output_path_csv)

ray.shutdown()
