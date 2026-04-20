# database/models.py — Schema documentation
#
# The three MySQL tables are created by database.db.init_db().
# This file documents the schema for reference.
#
# seen_tenders        — deduplication index
#   tender_id (UNIQUE), title, source_site, url, date_first_seen, notified
#
# tender_intelligence — AI enrichment layer
#   tender_id (UNIQUE), fit_score, semantic_score, keyword_score,
#   ai_summary, fit_explanation, fit_reasons (JSON), sector, geography,
#   service_type (JSON), client_org, budget_usd, deadline_extracted,
#   is_goods_only, red_flags (JSON), embedding_id, processed_at
#
# tenders             — normalised records
#   tender_id (UNIQUE), content_hash, source_portal, url, title,
#   title_clean (FULLTEXT), organization, country, deadline (DATE),
#   deadline_raw, description (MEDIUMTEXT), word_count, has_description,
#   sectors (JSON), service_types (JSON), primary_sector, fit_score,
#   semantic_score, keyword_score, fit_explanation, top_reasons (JSON),
#   red_flags (JSON), estimated_budget_usd, is_duplicate, duplicate_of,
#   is_expired, scraped_at, updated_at
