# intelligence/ — Data normalization and classification layer for TenderRadar
#
# Pipeline order (called in intelligence_layer.process_batch):
#
#   Raw dict (from scraper)
#       ↓
#   NormalizedTender  (normalizer.py)   ← deterministic, always runs, no API
#       ↓
#   Classification    (classifier.py)   ← rule-based, always runs, no API
#       ↓
#   TenderExtraction  (intelligence_layer.py) ← GPT-4o, optional
#       ↓
#   DedupResult       (deduplicator.py) ← fingerprint + semantic, always runs
#       ↓
#   EnrichedTender    (intelligence_layer.py) ← final merged record
#       ↓
#   tenders table (db.py)  +  tender_intelligence table (db.py)

from intelligence.normalizer  import NormalizedTender, normalize_tender
from intelligence.classifier  import Classification, classify_tender
from intelligence.deduplicator import DedupResult, check_duplicate

__all__ = [
    "NormalizedTender", "normalize_tender",
    "Classification",   "classify_tender",
    "DedupResult",      "check_duplicate",
]

from intelligence.keywords      import FIRM_EXPERTISE, score_relevance, title_is_relevant, score_tender_numeric
from intelligence.vector_store  import is_duplicate, store_tender, find_similar_tenders
# intelligence_layer NOT re-exported here — it is optional/heavy, import directly
