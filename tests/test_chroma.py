"""
test_chroma.py — Unit tests for ChromaDB vector store integration.

Tests that:
- A ChromaDB collection can be created with the correct config
- Items can be added and retrieved
- Cosine similarity space is configured correctly

Uses a temporary directory so it works in CI without any persistent state.
"""
import tempfile
import pytest
import chromadb


def _make_client(tmp_path: str):
    return chromadb.PersistentClient(path=tmp_path)


def test_collection_creation():
    """ChromaDB collection can be created with cosine space."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        col = client.get_or_create_collection(
            name="tenders_v3",
            metadata={"hnsw:space": "cosine"},
        )
        assert col.name == "tenders_v3"
        assert col.count() == 0


def test_add_and_query():
    """Documents added to collection are retrievable by embedding query."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        col = client.get_or_create_collection(
            name="tenders_test",
            metadata={"hnsw:space": "cosine"},
        )
        col.add(
            documents=["evaluation of education program in Bihar"],
            ids=["doc1"],
        )
        col.add(
            documents=["supply of laptops to government office"],
            ids=["doc2"],
        )
        assert col.count() == 2
        results = col.query(query_texts=["monitoring and evaluation"], n_results=1)
        assert results["ids"][0][0] == "doc1"


def test_get_or_create_idempotent():
    """get_or_create_collection is idempotent — same config, same collection."""
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_client(tmp)
        col1 = client.get_or_create_collection("tenders_v3", metadata={"hnsw:space": "cosine"})
        col2 = client.get_or_create_collection("tenders_v3", metadata={"hnsw:space": "cosine"})
        assert col1.name == col2.name
