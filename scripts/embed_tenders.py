#!/usr/bin/env python3
# =============================================================================
# scripts/embed_tenders.py — Semantic Brain Builder
#
# Pulls all tenders from MySQL (seen_tenders + tender_structured_intel)
# and pushes them through PyTorch sentence-transformers to build the
# ChromaDB Vector Store. This powers the RAG Chatbot.
# =============================================================================

import sys
import os
import time

# Ensure we can import from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.db import get_connection
from intelligence.vector_store import index_tenders_batch

def main():
    print("🚀 Starting Vector Embedding Engine...")
    print("This will embed all tenders into ChromaDB for Semantic RAG Search.\n")
    
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    
    # Get total count
    cur.execute("SELECT COUNT(*) as count FROM seen_tenders")
    total_tenders = cur.fetchone()["count"]
    print(f"📦 Total Tenders in Database: {total_tenders}")
    
    # We will fetch in batches to save memory
    BATCH_SIZE = 500
    offset = 0
    total_embedded = 0
    
    start_time = time.time()
    
    while offset < total_tenders:
        print(f"\nFetching batch {offset} to {offset + BATCH_SIZE}...")
        cur.execute(f"""
            SELECT 
                st.tender_id, 
                st.title, 
                st.url, 
                st.source_site,
                si.sector,
                si.region,
                si.opportunity_insight,
                si.relevance_score,
                si.priority_score
            FROM seen_tenders st
            LEFT JOIN tender_structured_intel si ON st.tender_id = si.tender_id
            ORDER BY st.id DESC
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """)
        
        rows = cur.fetchall()
        if not rows:
            break
            
        tenders_to_embed = []
        for row in rows:
            # Build a rich semantic description
            sector = row.get("sector") or "General"
            region = row.get("region") or "Global"
            insight = row.get("opportunity_insight") or ""
            
            # Build rich text — always lead with the raw title for keyword matching,
            # then append structured metadata only when it adds real information.
            title = row.get("title") or ""
            parts = [title]
            if sector != "General":
                parts.append(f"Sector: {sector}")
            if region != "Global":
                parts.append(f"Region: {region}")
            if insight:
                parts.append(insight)
            rich_description = ". ".join(parts).strip()
            
            tenders_to_embed.append({
                "tender_id": row["tender_id"],
                "title": row["title"],
                "description": rich_description,
                "source_site": row["source_site"],
                "url": row.get("url") or "",
                "relevance_score": row.get("priority_score") or row.get("relevance_score") or 0
            })
            
        # Push through the PyTorch ingestion pipeline
        print(f"🧠 Embedding {len(tenders_to_embed)} tenders via all-MiniLM-L6-v2...")
        t0 = time.perf_counter()
        stored = index_tenders_batch(tenders_to_embed)
        t1 = time.perf_counter()
        
        total_embedded += stored
        print(f"✅ Batch embedded successfully in {t1-t0:.2f}s (Speed: {len(tenders_to_embed)/(t1-t0):.1f} docs/sec)")
        
        offset += BATCH_SIZE
        
    cur.close()
    conn.close()
    
    total_time = time.time() - start_time
    print(f"\n🎉 EMBEDDING COMPLETE!")
    print(f"Total Tenders Indexed: {total_embedded}/{total_tenders}")
    print(f"Total Time Taken: {total_time/60:.2f} minutes")

if __name__ == "__main__":
    main()
