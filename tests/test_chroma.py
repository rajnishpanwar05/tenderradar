import chromadb
client = chromadb.PersistentClient(path="/Users/rajnishpanwar/tender_system/chroma_db")
collection = client.get_or_create_collection(
    name="tenders_v3",
    metadata={"hnsw:space": "cosine"}
)
print(f"Total Tenders embedded in Vector DB: {collection.count()}")
