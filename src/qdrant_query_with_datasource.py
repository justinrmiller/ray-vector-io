import ray
from ray.data import read_datasource

from qdrant import qdrant_datasource

ray.init()

HOST = "localhost"
PORT = 6333
COLLECTION_NAME = "collection"

qdrant_datasource = qdrant_datasource.QdrantDatasource(
    host=HOST,
    port=PORT,
    collection_name=COLLECTION_NAME,
    with_payload=True, 
    with_vectors=True,
    limit=10_000,
)

dataset = read_datasource(
    qdrant_datasource,
    parallelism=1
)
df = dataset.to_pandas()

output_path_parquet = "../parquet/output.parquet"
df.to_parquet(output_path_parquet, index=False)

output_path_csv = "../csv/output.csv"
df.to_csv(output_path_csv)

ray.shutdown()
