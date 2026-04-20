import sys
import logging
from intelligence.vector_store import find_similar_tenders

logging.basicConfig(level=logging.DEBUG)

def main():
    q = "give me top 10 education projects in india or south asia specifically ?"
    print("Running search for:", q)
    try:
        res = find_similar_tenders(q, top_k=60)
        print(f"Results list size: {len(res)}")
    except Exception as e:
        print(f"EXCEPTION: {e}")

if __name__ == '__main__':
    main()
