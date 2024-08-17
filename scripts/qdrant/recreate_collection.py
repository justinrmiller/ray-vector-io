from qdrant_client import QdrantClient

# Configuration parameters
collection_name = "collection"
embedding_size = 768

# Initialize Qdrant client
client = QdrantClient(host="localhost", port=6333)

if collection_name in [collection.name for collection in client.get_collections().collections]:
    client.delete_collection(collection_name=collection_name)

# Create the collection
client.create_collection(
    collection_name=collection_name,
    vectors_config={"size": embedding_size, "distance": "Cosine"},
)
