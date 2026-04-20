import sys
import os
sys.path.insert(0, "/Users/rajnishpanwar/tender_system")
from database.db import get_connection

conn = get_connection()
cur = conn.cursor(dictionary=True)
cur.execute("""
    SELECT source_site, MIN(url) as url
    FROM seen_tenders 
    GROUP BY source_site
""")
for row in cur.fetchall():
    print(f"{row['source_site']}: {row['url']}")
cur.close()
conn.close()
