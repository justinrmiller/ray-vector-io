import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

# Configuration parameters
collection_name = "collection"
embedding_size = 768
num_embeddings = 100_000
batch_size = 200

# Initialize Qdrant client
client = QdrantClient(host="localhost", port=6333)

# Generate random embeddings
random_embeddings = np.random.rand(num_embeddings, embedding_size).tolist()

# Prepare points to insert
points = [
    PointStruct(id=i, payload={"id": i}, vector=embedding)
    for i, embedding in enumerate(random_embeddings)
]

# Insert points into the collection in batches
for i in range(0, num_embeddings, batch_size):
    batch = points[i : i + batch_size]
    client.upsert(collection_name=collection_name, points=batch)
    print(f"Inserted batch {i//batch_size + 1} of {num_embeddings // batch_size}")

print(
    f"Inserted {num_embeddings} random embeddings into the '{collection_name}' collection."
)
