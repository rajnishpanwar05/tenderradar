# =============================================================================
# db.py — MySQL helpers for the Tender Monitoring System
# =============================================================================

import logging
import contextlib
import socket
import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from typing import Optional
from config.config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASS,
    DB_NAME,
    DB_POOL_SIZE,
    get_db_config,
)

_log = logging.getLogger("tenderradar.db")

# =============================================================================
# DRY-RUN GUARD
# When True, all write operations become no-ops so --dry-run never pollutes
# the seen_tenders or tenders tables with test data.
#
# Set from main.py BEFORE any scrapers or DB calls:
#   import db as _db; _db.DRY_RUN = True
# =============================================================================
DRY_RUN: bool = False


# ── Timeouts ──────────────────────────────────────────────────────────────────
# Max time any single SQL statement may run before MySQL forcibly aborts it.
# Prevents a slow/hung query from stalling a scraper thread indefinitely.
# Requires MySQL 5.7.7+. Silently skipped on older versions.
_QUERY_TIMEOUT_MS: int = 30_000   # 30 seconds per statement


def _apply_session_defaults(conn) -> None:
    """
    Run once per connection after it is acquired from the pool.
    Sets session-level guardrails that protect every query on this connection.
    """
    try:
        cur = conn.cursor()
        cur.execute(f"SET SESSION max_execution_time = {_QUERY_TIMEOUT_MS};")
        cur.close()
    except Error as exc:
        # Older MySQL / MariaDB may not support max_execution_time — non-fatal.
        _log.debug("[db] max_execution_time not supported on this server: %s", exc)


class DatabasePreflightError(RuntimeError):
    """Readable startup failure for local DB configuration issues."""

    def __init__(self, message: str, debug_detail: str = ""):
        super().__init__(message)
        self.debug_detail = debug_detail


def get_db_connection_summary(mask_password: bool = True) -> dict[str, object]:
    cfg = dict(get_db_config())
    if mask_password:
        password = str(cfg.get("password") or "")
        cfg["password"] = "***" if password else ""
    return cfg


def _format_mysql_config_target() -> str:
    return f"{DB_HOST}:{DB_PORT}"


def _socket_probe(timeout: float) -> None:
    try:
        with socket.create_connection((DB_HOST, DB_PORT), timeout=timeout):
            return
    except socket.gaierror as exc:
        raise DatabasePreflightError(
            f"DB host '{DB_HOST}' could not be resolved. Check DB_HOST in .env.",
            debug_detail=repr(exc),
        ) from exc
    except ConnectionRefusedError as exc:
        raise DatabasePreflightError(
            f"DB configured for {_format_mysql_config_target()} but nothing is listening there. "
            "Try starting MySQL or updating DB_PORT in .env.",
            debug_detail=repr(exc),
        ) from exc
    except socket.timeout as exc:
        raise DatabasePreflightError(
            f"DB configured for {_format_mysql_config_target()} but the connection timed out. "
            "Check DB_HOST/DB_PORT and make sure MySQL is reachable.",
            debug_detail=repr(exc),
        ) from exc
    except OSError as exc:
        raise DatabasePreflightError(
            f"DB configured for {_format_mysql_config_target()} but the socket check failed. "
            "Try starting MySQL or updating DB_HOST/DB_PORT in .env.",
            debug_detail=repr(exc),
        ) from exc


def preflight_db_connection(timeout: float = 3.0, debug: bool = False) -> dict[str, object]:
    """
    Validate that the configured MySQL host/port is reachable and credentials are usable.

    Returns a small status dict for startup logs and scripts.
    Raises DatabasePreflightError with a user-readable message on failure.
    """
    _socket_probe(timeout=timeout)

    base_kwargs = dict(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        connection_timeout=max(3, int(timeout) + 2),
        autocommit=True,
    )

    try:
        conn = mysql.connector.connect(database=DB_NAME, **base_kwargs)
        conn.close()
        return {
            "reachable": True,
            "database_exists": True,
            "host": DB_HOST,
            "port": DB_PORT,
            "user": DB_USER,
            "database": DB_NAME,
        }
    except Error as exc:
        if getattr(exc, "errno", None) == 1049:
            try:
                conn = mysql.connector.connect(**base_kwargs)
                conn.close()
                return {
                    "reachable": True,
                    "database_exists": False,
                    "host": DB_HOST,
                    "port": DB_PORT,
                    "user": DB_USER,
                    "database": DB_NAME,
                }
            except Error as admin_exc:
                detail = repr(admin_exc) if debug else ""
                raise DatabasePreflightError(
                    f"MySQL is reachable at {_format_mysql_config_target()}, but database "
                    f"'{DB_NAME}' is missing and the configured user cannot create it. "
                    "Create the database or update DB_NAME/credentials in .env.",
                    debug_detail=detail,
                ) from admin_exc

        detail = repr(exc) if debug else ""
        errno = getattr(exc, "errno", None)
        if errno == 1045:
            raise DatabasePreflightError(
                f"MySQL rejected DB_USER='{DB_USER}' for {_format_mysql_config_target()}. "
                "Check DB_USER and DB_PASSWORD in .env.",
                debug_detail=detail,
            ) from exc
        if errno == 1044:
            raise DatabasePreflightError(
                f"MySQL reached {_format_mysql_config_target()}, but DB_USER='{DB_USER}' "
                f"does not have access to database '{DB_NAME}'. Update DB_NAME or grant access.",
                debug_detail=detail,
            ) from exc
        if errno in (2003, 2005):
            raise DatabasePreflightError(
                f"MySQL could not connect to {_format_mysql_config_target()}. "
                "Try starting MySQL or updating DB_HOST/DB_PORT in .env.",
                debug_detail=detail,
            ) from exc
        raise DatabasePreflightError(
            f"MySQL preflight failed for {_format_mysql_config_target()} / database '{DB_NAME}'. "
            "Check your DB settings in .env.",
            debug_detail=detail or str(exc),
        ) from exc


# ── Connection ────────────────────────────────────────────────────────────────

def get_connection(retries: int = 3, backoff: float = 1.0):
    """
    Return a live MySQL connection with session-level query timeout applied.

    Raises:
        mysql.connector.Error: after all retries are exhausted.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max(1, retries)):
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                connection_timeout=15,   # slightly longer — docker port-mapping adds ~50ms
                # No pool_name: direct connection avoids pool-queue saturation
                # under parallel scraper threads.  Each caller owns its socket
                # and closes it when done, keeping total open connections low.
                autocommit=False,
            )
            _apply_session_defaults(conn)
            return conn
        except Error as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)   # 1s → 2s → 4s
                _log.warning(
                    "get_connection attempt %d/%d failed (%s) — retrying in %.1fs",
                    attempt + 1, retries, exc, wait,
                )
                time.sleep(wait)
    raise last_exc


@contextlib.contextmanager
def db_transaction():
    """
    Context manager for safe DB write operations.

    Usage:
        with db_transaction() as (conn, cur):
            cur.execute("INSERT INTO ...")
            # auto-committed on success, rolled-back + closed on any exception

    Guarantees:
      - Connection is ALWAYS closed (no leaks).
      - Transaction is committed on clean exit.
      - Transaction is rolled back on any exception, then re-raises.
      - Works with the session timeout set by _apply_session_defaults().
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def init_db():
    """
    Create the database (if it doesn't exist) and the seen_tenders table.
    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS.
    """
    # 1) Prefer connecting to the target DB directly with retries.
    # 2) Only attempt CREATE DATABASE when we *know* the DB is missing (errno 1049).
    # 3) After CREATE, retry normal DB connect before proceeding.
    direct_ok = False
    need_create = False
    last_err: Optional[Error] = None

    for _ in range(15):
        try:
            conn = get_connection()
            conn.close()
            direct_ok = True
            break
        except Error as e:
            last_err = e
            if getattr(e, "errno", None) == 1049:
                need_create = True
                break
            time.sleep(1.0)

    if need_create:
        created = False
        for _ in range(5):
            try:
                conn = mysql.connector.connect(
                    host=DB_HOST, port=DB_PORT,
                    user=DB_USER, password=DB_PASS,
                    connection_timeout=10,
                )
                cur = conn.cursor()
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
                conn.commit()
                cur.close()
                conn.close()
                created = True
                break
            except Error as e:
                last_err = e
                time.sleep(1.0)
        if not created:
            raise RuntimeError(f"Could not create database '{DB_NAME}': {last_err}")

        for _ in range(15):
            try:
                conn = get_connection()
                conn.close()
                direct_ok = True
                break
            except Error as e:
                last_err = e
                time.sleep(1.0)

    if not direct_ok:
        raise RuntimeError(
            f"Could not connect to database '{DB_NAME}' after retries: {last_err}"
        )

    # Now connect to the database and create the table
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_tenders (
                id              INT AUTO_INCREMENT PRIMARY KEY,
                tender_id       VARCHAR(255) UNIQUE,
                title           TEXT,
                source_site     VARCHAR(100),
                url             TEXT,
                date_first_seen DATETIME,
                notified        BOOLEAN DEFAULT FALSE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        conn.close()
        _log.info("[db] Database '%s' and table 'seen_tenders' ready.", DB_NAME)
    except Error as e:
        raise RuntimeError(f"Could not create table: {e}")

    # Intelligence layer schema (non-fatal if it fails)
    init_intelligence_schema()

    # Normalised tenders table (non-fatal if it fails)
    init_tenders_schema()

    # Cross-portal hash fingerprints (non-fatal if it fails)
    from intelligence.deduplicator import init_hash_schema
    init_hash_schema()

    # Cross-portal fuzzy merge table — tender_cross_sources (non-fatal)
    try:
        from intelligence.fuzzy_dedup import init_cross_sources_schema as _init_cs
        _init_cs()
    except Exception as _cs_e:
        _log.debug("[db] tender_cross_sources schema init skipped (non-fatal): %s", _cs_e)

    # Structured tender intelligence table (non-fatal if it fails)
    try:
        from intelligence.tender_intelligence import init_schema as _init_si
        _init_si()
    except Exception as _e:
        _log.warning("[db] tender_structured_intel schema warning (non-fatal): %s", _e)

    # Opportunity engine columns — priority_score, competition_level, opportunity_size,
    # complexity_score.  Called here so the API never starts with a schema gap,
    # even if the scraper pipeline has never run on this machine.
    try:
        from intelligence.opportunity_engine import extend_schema as _extend_opp
        _extend_opp()
    except Exception as _e:
        _log.warning("[db] opportunity_engine schema warning (non-fatal): %s", _e)

    # Opportunity insight column (TEXT — human-readable strategic summary per tender)
    try:
        from intelligence.opportunity_insights import extend_schema as _extend_oi
        _extend_oi()
    except Exception as _e:
        _log.warning("[db] opportunity_insights schema warning (non-fatal): %s", _e)

    # Bid pipeline tracking table (non-fatal if it fails)
    try:
        from pipeline.opportunity_pipeline import initialize_pipeline_table as _init_bp
        _init_bp()
    except Exception as _e:
        _log.warning("[db] bid_pipeline schema warning (non-fatal): %s", _e)

    # Amendment tracking + deep extraction columns in tenders (non-fatal)
    # Must run BEFORE _init_unified_view() so the view can reference these columns.
    _add_amendment_columns_if_missing()

    # Unified view — v_tender_full (non-fatal; MySQL 5.7+)
    # Called AFTER column additions so the view can safely reference all columns.
    _init_unified_view()

    # data_confidence_score column in tender_structured_intel (non-fatal)
    _add_confidence_column_if_missing()

    # decision_tag column in tender_structured_intel (non-fatal)
    _add_decision_tag_column_if_missing()

    # Performance indexes — idempotent, non-fatal
    _ensure_performance_indexes()

    # World Bank early pipeline table (non-fatal)
    try:
        init_wb_early_schema()
    except Exception as _e:
        _log.warning("[db] world_bank_early_pipeline schema warning (non-fatal): %s", _e)

    # Generic pre-signal opportunity table (non-fatal)
    try:
        init_opportunity_signals_schema()
    except Exception as _e:
        _log.warning("[db] opportunity_signals schema warning (non-fatal): %s", _e)


def _add_amendment_columns_if_missing() -> None:
    """
    Add all extended deep-scraper columns to the tenders table if they
    do not already exist.

    Covers two sets of columns added after the initial schema was created:

    Amendment tracking (deep_scraper.py — amendment detection):
      - document_hash              VARCHAR(32)   — MD5 of last-seen combined text
      - amendment_count            INT           — how many times content changed
      - last_amended_at            DATETIME      — timestamp of most recent change

    Upgraded extraction (deep_scraper.py — structured PDF extraction, Task 2):
      - deep_budget_currency       VARCHAR(10)   — e.g. USD / INR / EUR
      - deep_date_pre_bid          VARCHAR(80)   — pre-bid conference date
      - deep_date_qa_deadline      VARCHAR(80)   — Q&A cut-off date
      - deep_date_contract_start   VARCHAR(80)   — expected contract start
      - deep_contract_duration     VARCHAR(120)  — e.g. "18 months"
      - deep_eval_technical_weight TINYINT       — e.g. 70 (technical %)
      - deep_eval_financial_weight TINYINT       — e.g. 30 (financial %)
      - deep_eligibility_raw       TEXT          — raw eligibility criteria block
      - deep_min_turnover_raw      VARCHAR(150)  — minimum turnover requirement
      - deep_min_years_experience  TINYINT       — minimum years of experience
      - deep_min_similar_projects  TINYINT       — minimum similar projects count
      - deep_contact_block         VARCHAR(400)  — contact info paragraph

    Idempotent — uses INFORMATION_SCHEMA checks. Non-fatal on any failure.
    """
    _cols = [
        # Amendment tracking
        ("document_hash",              "VARCHAR(32) NOT NULL DEFAULT ''"),
        ("amendment_count",            "INT NOT NULL DEFAULT 0"),
        ("last_amended_at",            "DATETIME DEFAULT NULL"),
        # Upgraded deep extraction
        ("deep_budget_currency",       "VARCHAR(10) DEFAULT ''"),
        ("deep_date_pre_bid",          "VARCHAR(80) DEFAULT ''"),
        ("deep_date_qa_deadline",      "VARCHAR(80) DEFAULT ''"),
        ("deep_date_contract_start",   "VARCHAR(80) DEFAULT ''"),
        ("deep_contract_duration",     "VARCHAR(120) DEFAULT ''"),
        ("deep_eval_technical_weight", "TINYINT DEFAULT NULL"),
        ("deep_eval_financial_weight", "TINYINT DEFAULT NULL"),
        ("deep_eligibility_raw",       "TEXT"),
        ("deep_min_turnover_raw",      "VARCHAR(150) DEFAULT ''"),
        ("deep_min_years_experience",  "TINYINT DEFAULT NULL"),
        ("deep_min_similar_projects",  "TINYINT DEFAULT NULL"),
        ("deep_contact_block",         "VARCHAR(400) DEFAULT ''"),
        ("deep_document_links",        "JSON"),
        ("deep_ai_summary",            "MEDIUMTEXT"),
    ]
    try:
        conn = get_connection()
        cur  = conn.cursor()

        for col_name, col_def in _cols:
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'tenders'
                  AND COLUMN_NAME  = %s;
            """, (col_name,))
            exists = (cur.fetchone() or (0,))[0] > 0

            if not exists:
                cur.execute(
                    f"ALTER TABLE tenders ADD COLUMN {col_name} {col_def};"
                )
                conn.commit()
                _log.info("[db] Column '%s' added to tenders table.", col_name)
            else:
                _log.debug("[db] Column '%s' already exists in tenders — skipping.", col_name)

        cur.close()
        conn.close()
    except Exception as e:
        try:
            conn.rollback()  # type: ignore[possibly-undefined]
            conn.close()
        except Exception:
            pass
        _log.warning("[db] amendment columns init warning (non-fatal): %s", e)


def _add_confidence_column_if_missing() -> None:
    """
    Add data_confidence_score (SMALLINT 0-100) to tender_structured_intel
    if the column does not already exist.

    Uses INFORMATION_SCHEMA instead of 'IF NOT EXISTS' so it works on all
    MySQL versions (5.7, 8.0, MariaDB, etc.).
    Non-fatal — any failure is logged but does not abort startup.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # Check whether the column already exists
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = 'tender_structured_intel'
              AND COLUMN_NAME  = 'data_confidence_score';
        """)
        exists = (cur.fetchone() or (0,))[0] > 0

        if not exists:
            cur.execute("""
                ALTER TABLE tender_structured_intel
                ADD COLUMN data_confidence_score SMALLINT NOT NULL DEFAULT 50;
            """)
            conn.commit()
            _log.info("[db] Column 'data_confidence_score' added to tender_structured_intel.")
        else:
            _log.debug("[db] Column 'data_confidence_score' already exists — skipping.")

        cur.close()
        conn.close()
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] data_confidence_score column warning (non-fatal): %s", e)


def _add_decision_tag_column_if_missing() -> None:
    """
    Add decision_tag VARCHAR(20) to tender_structured_intel if the column
    does not already exist.

    Stores the tier label (BID_NOW | STRONG_CONSIDER | WEAK_CONSIDER | IGNORE)
    so dashboards and opportunity pipelines can filter without re-scoring.

    Non-fatal — any failure is logged but does not abort startup.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = 'tender_structured_intel'
              AND COLUMN_NAME  = 'decision_tag';
        """)
        exists = (cur.fetchone() or (0,))[0] > 0

        if not exists:
            cur.execute("""
                ALTER TABLE tender_structured_intel
                ADD COLUMN decision_tag VARCHAR(20) NOT NULL DEFAULT 'IGNORE';
            """)
            conn.commit()
            _log.info("[db] Column 'decision_tag' added to tender_structured_intel.")
        else:
            _log.debug("[db] Column 'decision_tag' already exists — skipping.")

        cur.close()
        conn.close()
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] decision_tag column warning (non-fatal): %s", e)


def _ensure_performance_indexes() -> None:
    """
    Create missing indexes on tender_structured_intel and seen_tenders
    that are critical for dashboard and search query performance.

    Each index is created with IF NOT EXISTS (MySQL 8.0+) or guarded by
    an INFORMATION_SCHEMA check so the function is fully idempotent.
    Non-fatal — any failure is logged but does not abort startup.

    Indexes created:
      tender_structured_intel:
        idx_deadline_category   (deadline_category)   — deadline breakdown queries
        idx_decision_tag        (decision_tag)         — tier filtering / performance
        idx_si_priority         (priority_score)       — already on SI table, safety net
      seen_tenders:
        idx_st_source_site      (source_site)          — portal filter in intel join
    """
    _indexes = [
        # (table, index_name, columns_sql)
        ("tender_structured_intel", "idx_deadline_category",  "(deadline_category)"),
        ("tender_structured_intel", "idx_decision_tag",       "(decision_tag)"),
        ("tender_structured_intel", "idx_si_priority",        "(priority_score)"),
        ("seen_tenders",            "idx_st_source_site",     "(source_site)"),
    ]
    try:
        conn = get_connection()
        cur  = conn.cursor()

        for table, idx_name, cols in _indexes:
            # Check whether the index already exists
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = %s
                  AND INDEX_NAME   = %s;
            """, (table, idx_name))
            already_exists = (cur.fetchone() or (0,))[0] > 0

            if not already_exists:
                try:
                    cur.execute(
                        f"CREATE INDEX {idx_name} ON {table} {cols};"
                    )
                    conn.commit()
                    _log.info("[db] Created index %s ON %s%s", idx_name, table, cols)
                except Exception as idx_err:
                    # Non-fatal per-index — log and continue
                    _log.warning("[db] Could not create index %s: %s", idx_name, idx_err)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            else:
                _log.debug("[db] Index %s already exists — skipping.", idx_name)

        cur.close()
        conn.close()
    except Exception as e:
        try:
            conn.close()  # type: ignore[possibly-undefined]
        except Exception:
            pass
        _log.warning("[db] _ensure_performance_indexes warning (non-fatal): %s", e)


# =============================================================================
# UNIFIED VIEW — v_tender_full
#
# Single SQL view that joins all four intel sources into one flat record.
# Replaces the need to write complex multi-table JOINs in every endpoint.
#
# Tables joined:
#   seen_tenders            st  — canonical source-of-truth for tender existence
#   tender_structured_intel si  — rule-based sector/region/priority scores
#   tenders                  t  — normalised fields + AI enrichment
#   tender_intelligence      ti  — GPT/embedding scores
#   bid_pipeline             bp  — current pipeline status per tender
#
# ALL joins are LEFT JOINs — a row exists in the view for every seen tender,
# even if no intelligence pipeline has processed it yet.
# =============================================================================

def _init_unified_view() -> None:
    """
    Create or replace the v_tender_full view.
    Uses CREATE OR REPLACE VIEW so it is always up-to-date with schema changes.
    Non-fatal — any failure is logged and the API continues without the view.
    """
    _VIEW_SQL = """
        CREATE OR REPLACE VIEW v_tender_full AS
        SELECT
            -- Identity
            st.tender_id,
            COALESCE(t.title_clean, st.title)                AS title,
            COALESCE(t.url, st.url)                          AS url,
            st.source_site,
            st.date_first_seen,
            st.notified,

            -- Organisation & geography
            COALESCE(si.organization, t.organization, '')    AS organization,
            COALESCE(t.country, si.region, 'global')         AS country,
            COALESCE(si.region, 'global')                    AS region,

            -- Classification
            COALESCE(si.sector, t.primary_sector, 'unknown') AS sector,
            COALESCE(si.consulting_type, 'unknown')           AS consulting_type,
            t.sectors                                         AS sectors_json,
            t.service_types                                   AS service_types_json,
            t.primary_sector,

            -- Deadline
            COALESCE(si.deadline_category, 'unknown')         AS deadline_category,
            t.deadline,
            t.deadline_raw,

            -- Scores (structured intel — fast, rule-based)
            COALESCE(si.relevance_score,   0)  AS relevance_score,
            COALESCE(si.priority_score,    0)  AS priority_score,
            COALESCE(si.complexity_score,  0)  AS complexity_score,
            COALESCE(si.competition_level, 'medium') AS competition_level,
            COALESCE(si.opportunity_size,  'medium') AS opportunity_size,
            si.opportunity_insight,
            COALESCE(si.is_consulting_relevant, 1) AS is_consulting_relevant,
            COALESCE(si.is_low_confidence, 0)      AS is_low_confidence,
            COALESCE(si.scoring_note, '')          AS scoring_note,
            COALESCE(si.client_fit_score, 0)       AS client_fit_score,
            COALESCE(si.service_fit_score, 0)      AS service_fit_score,
            COALESCE(si.consulting_confidence_score, 0) AS consulting_confidence_score,
            COALESCE(si.procurement_penalty_score, 0)   AS procurement_penalty_score,
            si.enriched_at,

            -- Scores (AI layer — GPT + embeddings)
            COALESCE(ti.fit_score, si.relevance_score, 0)    AS fit_score,
            COALESCE(t.fit_score,  ti.fit_score, 0)          AS tenders_fit_score,
            ti.semantic_score,
            ti.keyword_score,
            ti.fit_explanation,
            COALESCE(t.fit_explanation, ti.fit_explanation)  AS best_fit_explanation,
            t.top_reasons,
            COALESCE(t.red_flags, ti.red_flags)              AS red_flags,
            COALESCE(ti.budget_usd, t.estimated_budget_usd)  AS estimated_budget_usd,
            ti.ai_summary,

            -- Content
            t.description,
            t.word_count,
            t.has_description,
            t.deep_description,
            t.deep_pdf_text,
            t.deep_document_links,
            t.deep_ai_summary,

            -- Deep scraper fields (populated by scrapers/deep_scraper.py)
            t.is_duplicate,
            t.duplicate_of,
            t.is_expired,
            t.scraped_at,
            t.updated_at,

            -- Amendment tracking (document change detection)
            t.document_hash,
            COALESCE(t.amendment_count, 0) AS amendment_count,
            t.last_amended_at,

            -- Deep extraction — bid-critical structured fields (Task 2)
            t.deep_scope,
            t.deep_budget_raw,
            t.deep_budget_currency,
            t.deep_date_pre_bid,
            t.deep_date_qa_deadline,
            t.deep_date_contract_start,
            t.deep_contract_duration,
            t.deep_eval_technical_weight,
            t.deep_eval_financial_weight,
            t.deep_eval_criteria,
            t.deep_eligibility_raw,
            t.deep_min_turnover_raw,
            t.deep_min_years_experience,
            t.deep_min_similar_projects,
            t.deep_team_reqs,
            t.deep_contact_block,

            -- Pipeline status
            bp.status                  AS pipeline_status,
            bp.owner                   AS pipeline_owner,
            bp.model_decision_tag,
            bp.bid_decision,
            bp.outcome,
            bp.proposal_deadline       AS bid_deadline,
            bp.notes                   AS pipeline_notes

        FROM seen_tenders                  st
        LEFT JOIN tender_structured_intel  si ON st.tender_id = si.tender_id
        LEFT JOIN tenders                   t  ON st.tender_id = t.tender_id
        LEFT JOIN tender_intelligence       ti ON st.tender_id = ti.tender_id
        LEFT JOIN bid_pipeline              bp ON st.tender_id = bp.tender_id;
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(_VIEW_SQL)
        conn.commit()
        cur.close()
        conn.close()
        _log.info("[db] Unified view 'v_tender_full' created/updated OK.")
    except Exception as e:
        try:
            conn.rollback(); conn.close()   # type: ignore[possibly-undefined]
        except Exception:
            pass
        _log.warning("[db] v_tender_full view init warning (non-fatal): %s", e)


# ── Core helpers ──────────────────────────────────────────────────────────────

def check_if_new(tender_id: str) -> bool:
    """
    Return True  if this tender_id has NEVER been seen before.
    Return False if it already exists in seen_tenders.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT 1 FROM seen_tenders WHERE tender_id = %s LIMIT 1;",
                    (str(tender_id),))
        found = cur.fetchone() is not None
        cur.close()
        conn.close()
        return not found
    except Error as e:
        _log.error("[db] ⚠ CRITICAL check_if_new error: %s — treating as SEEN to avoid duplicates", e)
        return False   # assume seen on DB failure — prevents duplicate spam notifications


def mark_as_seen(tender_id: str, title: str, source_site: str, url: str,
                 notified: bool = True) -> bool:
    """
    Insert a new tender into seen_tenders.
    Uses INSERT IGNORE so duplicate calls are safe.
    Returns True on success.
    In DRY_RUN mode: silently skips the write and returns True.
    """
    if DRY_RUN:
        _log.info("[db][DRY-RUN] mark_as_seen skipped for: %s", tender_id[:60])
        return True
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            INSERT IGNORE INTO seen_tenders
                (tender_id, title, source_site, url, date_first_seen, notified)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            str(tender_id),
            str(title)[:1000],
            str(source_site)[:100],
            str(url)[:2000],
            datetime.now(),
            int(notified),
        ))
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        return affected > 0
    except Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log.error("[db] mark_as_seen error: %s", e)
        return False


def get_stats() -> dict:
    """Return a quick summary of the seen_tenders table."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*), source_site FROM seen_tenders GROUP BY source_site;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {site: count for count, site in rows}
    except Error as e:
        _log.error("[db] get_stats error: %s", e)
        return {}


# =============================================================================
# INTELLIGENCE LAYER — tender_intelligence table
# =============================================================================

def init_intelligence_schema():
    """
    Create the tender_intelligence table for storing AI enrichment results.
    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS.
    Called automatically from init_db() so no manual invocation needed.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tender_intelligence (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                tender_id           VARCHAR(255)  UNIQUE,
                fit_score           FLOAT,
                semantic_score      FLOAT,
                keyword_score       FLOAT,
                ai_summary          TEXT,
                fit_explanation     TEXT,
                fit_reasons         TEXT,
                sector              JSON,
                geography           JSON,
                service_type        JSON,
                client_org          VARCHAR(255),
                budget_usd          INT,
                deadline_extracted  DATE,
                is_goods_only       BOOLEAN       DEFAULT FALSE,
                red_flags           JSON,
                embedding_id        VARCHAR(100),
                processed_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_fit_score    (fit_score),
                INDEX idx_processed_at (processed_at)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        conn.close()
        _log.info("[db] Table 'tender_intelligence' ready.")
    except Error as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] init_intelligence_schema warning (non-fatal): %s", e)


def save_intelligence(tender_id: str, enriched) -> bool:
    """
    Persist an EnrichedTender's AI intelligence data to tender_intelligence.
    Uses INSERT ... ON DUPLICATE KEY UPDATE so re-processing is idempotent.

    Args:
        tender_id: Stable unique identifier matching seen_tenders.tender_id.
        enriched:  EnrichedTender object from intelligence_layer.process_batch().

    Returns:
        True on success, False on any DB error.
    In DRY_RUN mode: silently skips the write and returns True.
    """
    if DRY_RUN:
        return True
    import json as _json

    try:
        conn = get_connection()
        cur  = conn.cursor()

        ext     = enriched.extraction
        summary = _json.dumps(ext.summary)  if (ext and ext.summary)  else None

        cur.execute("""
            INSERT INTO tender_intelligence
                (tender_id, fit_score, semantic_score, keyword_score,
                 ai_summary, fit_explanation, fit_reasons,
                 sector, geography, service_type, client_org,
                 budget_usd, deadline_extracted, is_goods_only,
                 red_flags, embedding_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                fit_score         = VALUES(fit_score),
                semantic_score    = VALUES(semantic_score),
                keyword_score     = VALUES(keyword_score),
                ai_summary        = VALUES(ai_summary),
                fit_explanation   = VALUES(fit_explanation),
                fit_reasons       = VALUES(fit_reasons),
                sector            = VALUES(sector),
                geography         = VALUES(geography),
                service_type      = VALUES(service_type),
                client_org        = VALUES(client_org),
                budget_usd        = VALUES(budget_usd),
                deadline_extracted = VALUES(deadline_extracted),
                is_goods_only     = VALUES(is_goods_only),
                red_flags         = VALUES(red_flags),
                embedding_id      = VALUES(embedding_id),
                processed_at      = CURRENT_TIMESTAMP;
        """, (
            str(tender_id)[:255],
            enriched.fit_score,
            enriched.semantic_score,
            enriched.keyword_score,
            summary,
            enriched.fit_explanation,
            _json.dumps(enriched.top_reasons),
            _json.dumps(ext.sector        if ext else []),
            _json.dumps(ext.geography     if ext else []),
            _json.dumps(ext.service_type  if ext else []),
            (ext.client_org               if ext else None),
            (ext.estimated_budget_usd     if ext else None),
            (ext.deadline[:10]            if (ext and ext.deadline) else None),
            (ext.is_goods_only            if ext else False),
            _json.dumps(enriched.red_flags),
            enriched.embedding_id,
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True

    except Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log.error("[db] save_intelligence error: %s", e)
        return False


# =============================================================================
# NORMALISED TENDERS TABLE
# =============================================================================

def init_tenders_schema() -> None:
    """
    Create the tenders table for storing fully normalised + enriched records.
    Safe to call repeatedly (CREATE TABLE IF NOT EXISTS).
    Called automatically by init_db().
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenders (
                id                   INT AUTO_INCREMENT PRIMARY KEY,
                tender_id            VARCHAR(255)   NOT NULL UNIQUE,
                content_hash         VARCHAR(32)    DEFAULT '',
                source_portal        VARCHAR(100)   NOT NULL,
                url                  TEXT,
                title                TEXT,
                title_clean          VARCHAR(500)   DEFAULT '',
                organization         VARCHAR(300)   DEFAULT '',
                country              VARCHAR(100)   DEFAULT '',
                deadline             DATE           DEFAULT NULL,
                deadline_raw         VARCHAR(100)   DEFAULT '',
                description          MEDIUMTEXT,
                word_count           INT            DEFAULT 0,
                has_description      BOOLEAN        DEFAULT FALSE,
                sectors              JSON,
                service_types        JSON,
                primary_sector       VARCHAR(50)    DEFAULT NULL,
                fit_score            FLOAT          DEFAULT 0.0,
                semantic_score       FLOAT          DEFAULT 0.0,
                keyword_score        FLOAT          DEFAULT 0.0,
                fit_explanation      TEXT,
                top_reasons          JSON,
                red_flags            JSON,
                estimated_budget_usd INT            DEFAULT NULL,
                is_duplicate         BOOLEAN        DEFAULT FALSE,
                duplicate_of         VARCHAR(255)   DEFAULT NULL,
                is_expired           BOOLEAN        DEFAULT FALSE,
                scraped_at           TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
                updated_at           TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_source_portal  (source_portal),
                INDEX idx_content_hash   (content_hash),
                INDEX idx_fit_score      (fit_score),
                INDEX idx_deadline       (deadline),
                INDEX idx_scraped_at     (scraped_at),
                INDEX idx_country        (country),
                FULLTEXT INDEX ft_title  (title_clean)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        conn.close()
        _ensure_tenders_columns()
        _log.info("[db] Table 'tenders' ready.")
    except Error as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] init_tenders_schema warning (non-fatal): %s", e)


def _ensure_tenders_columns() -> None:
    """
    Add post-v1 columns to `tenders` if they are missing.

    This keeps older databases compatible after new enrichment layers are added.
    Safe to call on every startup.
    """
    _cols = [
        ("value_raw", "VARCHAR(200) DEFAULT ''"),
        # Deep enrichment core fields
        ("deep_description", "MEDIUMTEXT"),
        ("deep_scope", "MEDIUMTEXT"),
        ("deep_budget_raw", "VARCHAR(200) DEFAULT ''"),
        ("deep_deadline_raw", "VARCHAR(100) DEFAULT ''"),
        ("deep_contact_emails", "JSON"),
        ("deep_eval_criteria", "TEXT"),
        ("deep_team_reqs", "TEXT"),
        ("deep_pdf_text", "MEDIUMTEXT"),
        ("deep_source", "VARCHAR(50) DEFAULT ''"),
        ("deep_scraped_at", "DATETIME DEFAULT NULL"),
        # Amendment + upgraded deep extraction fields
        ("document_hash", "VARCHAR(32) NOT NULL DEFAULT ''"),
        ("amendment_count", "INT NOT NULL DEFAULT 0"),
        ("last_amended_at", "DATETIME DEFAULT NULL"),
        ("deep_budget_currency", "VARCHAR(10) DEFAULT ''"),
        ("deep_date_pre_bid", "VARCHAR(80) DEFAULT ''"),
        ("deep_date_qa_deadline", "VARCHAR(80) DEFAULT ''"),
        ("deep_date_contract_start", "VARCHAR(80) DEFAULT ''"),
        ("deep_contract_duration", "VARCHAR(120) DEFAULT ''"),
        ("deep_eval_technical_weight", "TINYINT DEFAULT NULL"),
        ("deep_eval_financial_weight", "TINYINT DEFAULT NULL"),
        ("deep_eligibility_raw", "TEXT"),
        ("deep_min_turnover_raw", "VARCHAR(150) DEFAULT ''"),
        ("deep_min_years_experience", "TINYINT DEFAULT NULL"),
        ("deep_min_similar_projects", "TINYINT DEFAULT NULL"),
        ("deep_contact_block", "VARCHAR(400) DEFAULT ''"),
        ("deep_document_links", "JSON"),
        ("deep_ai_summary", "MEDIUMTEXT"),
    ]
    try:
        conn = get_connection()
        cur = conn.cursor()
        for col_name, col_def in _cols:
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'tenders'
                  AND COLUMN_NAME  = %s;
            """, (col_name,))
            if not (cur.fetchone() or (0,))[0]:
                cur.execute(f"ALTER TABLE tenders ADD COLUMN {col_name} {col_def};")
                conn.commit()
                _log.info("[db] Added column '%s' to tenders.", col_name)
        cur.close()
        conn.close()
    except Exception as e:
        _log.warning("[db] _ensure_tenders_columns warning (non-fatal): %s", e)


def save_normalized_tender(normalized: "NormalizedTender") -> bool:
    """
    Upsert a NormalizedTender into the tenders table.
    Uses INSERT … ON DUPLICATE KEY UPDATE so re-processing is idempotent.

    Args:
        normalized: NormalizedTender dataclass from intelligence.normalizer.

    Returns:
        True on success, False on any DB error.
    In DRY_RUN mode: silently skips the write and returns True.
    """
    if DRY_RUN:
        return True
    import json as _json
    from datetime import date as _date

    try:
        d = normalized.to_dict()
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO tenders
                (tender_id, content_hash, source_portal, url,
                 title, title_clean, organization, country,
                 deadline, deadline_raw, description, value_raw,
                 word_count, has_description, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                content_hash    = VALUES(content_hash),
                source_portal   = VALUES(source_portal),
                url             = VALUES(url),
                title           = VALUES(title),
                title_clean     = VALUES(title_clean),
                organization    = VALUES(organization),
                country         = VALUES(country),
                deadline        = VALUES(deadline),
                deadline_raw    = VALUES(deadline_raw),
                description     = VALUES(description),
                value_raw       = VALUES(value_raw),
                word_count      = VALUES(word_count),
                has_description = VALUES(has_description),
                scraped_at      = VALUES(scraped_at);
        """, (
            d["tender_id"],
            d["content_hash"],
            d["source_portal"],
            d["url"],
            d["title"],
            d["title_clean"],
            d["organization"],
            d["country"],
            d["deadline"],
            d["deadline_raw"],
            d["description"],
            d["value_raw"],
            d["word_count"],
            int(d["has_description"]),
            d["scraped_at"],
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log.error("[db] save_normalized_tender error: %s", e)
        return False


def materialize_normalized_batch(raw_tenders: list) -> dict:
    """
    Normalize and persist raw scraper output into `tenders` as early as possible.

    This protects descriptions and URLs from being lost if later AI/intelligence
    stages fail. Returns summary counts for run logging.
    """
    if not raw_tenders:
        return {"saved": 0, "failed": 0, "total": 0}

    try:
        from intelligence.normalizer import normalize_tender
    except Exception as exc:
        _log.error("[db] materialize_normalized_batch import error: %s", exc)
        return {"saved": 0, "failed": len(raw_tenders), "total": len(raw_tenders)}

    saved = 0
    failed = 0
    for raw in raw_tenders:
        try:
            tender_id = str(
                raw.get("tender_id") or raw.get("id") or raw.get("sol_num")
                or raw.get("Bid Number") or ""
            ).strip()
            normalized = normalize_tender(raw, tender_id=tender_id)
            if save_normalized_tender(normalized):
                saved += 1
            else:
                failed += 1
        except Exception as exc:
            _log.warning("[db] materialize_normalized_batch skipped one row: %s", exc)
            failed += 1

    return {"saved": saved, "failed": failed, "total": len(raw_tenders)}


def backfill_normalized_from_seen_tenders(limit: int = 10_000) -> int:
    """
    Create missing `tenders` rows from historical `seen_tenders` records.

    Historical rows will have thin descriptions because `seen_tenders` stores
    minimal fields, but this at least closes the raw→normalized gap and makes
    more records available to downstream APIs consistently.
    """
    try:
        from intelligence.normalizer import normalize_tender
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                st.tender_id,
                st.title,
                st.source_site AS source_portal,
                st.url,
                st.date_first_seen AS scraped_at
            FROM seen_tenders st
            LEFT JOIN tenders t ON st.tender_id = t.tender_id
            WHERE t.tender_id IS NULL
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        written = 0
        for row in rows:
            normalized = normalize_tender(
                {
                    "tender_id": row.get("tender_id"),
                    "title": row.get("title"),
                    "source_portal": row.get("source_portal"),
                    "url": row.get("url"),
                },
                tender_id=str(row.get("tender_id") or ""),
            )
            if save_normalized_tender(normalized):
                written += 1
        _log.info("[db] backfill_normalized_from_seen_tenders wrote %d/%d rows", written, len(rows))
        return written
    except Exception as exc:
        _log.warning("[db] backfill_normalized_from_seen_tenders failed: %s", exc)
        return 0


def update_tender_enrichment(
    tender_id:     str,
    sectors:       list,
    service_types: list,
    primary_sector: "Optional[str]",
    fit_score:     float,
    semantic_score: float,
    keyword_score:  float,
    fit_explanation: str,
    top_reasons:   list,
    red_flags:     list,
    estimated_budget_usd: "Optional[int]" = None,
    is_duplicate:  bool = False,
    duplicate_of:  "Optional[str]" = None,
    is_expired:    bool = False,
) -> bool:
    """
    Write AI enrichment + classification fields back to the tenders row.
    Called after intelligence_layer.process_batch() completes.
    """
    if DRY_RUN:
        return True
    import json as _json
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE tenders SET
                sectors               = %s,
                service_types         = %s,
                primary_sector        = %s,
                fit_score             = %s,
                semantic_score        = %s,
                keyword_score         = %s,
                fit_explanation       = %s,
                top_reasons           = %s,
                red_flags             = %s,
                estimated_budget_usd  = %s,
                is_duplicate          = %s,
                duplicate_of          = %s,
                is_expired            = %s
            WHERE tender_id = %s;
        """, (
            _json.dumps(sectors),
            _json.dumps(service_types),
            primary_sector,
            fit_score,
            semantic_score,
            keyword_score,
            fit_explanation,
            _json.dumps(top_reasons),
            _json.dumps(red_flags),
            estimated_budget_usd,
            int(is_duplicate),
            duplicate_of,
            int(is_expired),
            tender_id,
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log.error("[db] update_tender_enrichment error: %s", e)
        return False


def get_tender(tender_id: str) -> "Optional[dict]":
    """
    Fetch a single tender by ID. Returns dict or None.

    Strategy:
      1. Try the fully-enriched `tenders` table first (normalised + AI scored).
      2. Fall back to `seen_tenders LEFT JOIN tender_structured_intel` so that
         tenders from portals that haven't been through the intelligence
         pipeline (UNDP, UNGM, GeM, etc.) are still viewable in the UI.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # ── Primary: enriched tenders table ──────────────────────────────
        cur.execute("SELECT * FROM tenders WHERE tender_id = %s LIMIT 1;",
                    (tender_id,))
        row = cur.fetchone()

        if row:
            cur.close()
            conn.close()
            return row

        # ── Fallback: seen_tenders + intel join ──────────────────────────
        cur.execute("""
            SELECT
                st.tender_id,
                ''                          AS content_hash,
                st.source_site              AS source_portal,
                st.url,
                st.title,
                ''                          AS title_clean,
                COALESCE(si.organization, '') AS organization,
                COALESCE(si.region, '')     AS country,
                NULL                        AS deadline,
                ''                          AS deadline_raw,
                ''                          AS description,
                0                           AS word_count,
                FALSE                       AS has_description,
                '[]'                        AS sectors,
                '[]'                        AS service_types,
                COALESCE(si.sector, '')     AS primary_sector,
                COALESCE(si.relevance_score, 0)  AS fit_score,
                0                           AS semantic_score,
                0                           AS keyword_score,
                ''                          AS fit_explanation,
                '[]'                        AS top_reasons,
                '[]'                        AS red_flags,
                NULL                        AS estimated_budget_usd,
                FALSE                       AS is_duplicate,
                NULL                        AS duplicate_of,
                st.date_first_seen          AS scraped_at,
                FALSE                       AS is_expired
            FROM seen_tenders st
            LEFT JOIN tender_structured_intel si
                   ON si.tender_id = st.tender_id
            WHERE st.tender_id = %s
            LIMIT 1;
        """, (tender_id,))
        row = cur.fetchone()

        cur.close()
        conn.close()
        return row

    except Error as e:
        _log.error("[db] get_tender error: %s", e)
        return None


def search_tenders(
    q:               str   = "",
    sectors:         list  = None,
    service_types:   list  = None,
    countries:       list  = None,
    source_portals:  list  = None,
    min_fit_score:   float = 0.0,
    exclude_expired: bool  = True,
    exclude_duplicates: bool = True,
    page:            int   = 1,
    page_size:       int   = 20,
    sort_by:         str   = "fit_score",
    sort_order:      str   = "desc",
) -> dict:
    """
    Filtered + paginated tender search.

    Returns:
        {"results": [dict, ...], "total": int, "page": int, "page_size": int}
    """
    import json as _json

    allowed_sort = {"fit_score", "scraped_at", "deadline", "title_clean"}
    if sort_by not in allowed_sort:
        sort_by = "fit_score"
    sort_order = "DESC" if sort_order.lower() != "asc" else "ASC"

    where_clauses = []
    params: list = []

    if q:
        # Require ALL tokens (AND semantics) by prefixing each with '+'.
        # Single token "water" → "+water*"
        # Multi-word "water sanitation India" → "+water* +sanitation* +India*"
        # This prevents OR-union matches that inflate result counts.
        _bool_q = " ".join(f"+{tok}*" for tok in q.split() if tok.strip())
        where_clauses.append("MATCH(title_clean) AGAINST (%s IN BOOLEAN MODE)")
        params.append(_bool_q)

    if sectors:
        # JSON_OVERLAPS checks if any requested sector is present in the JSON array
        where_clauses.append(
            "JSON_OVERLAPS(sectors, %s)"
        )
        params.append(_json.dumps(sectors))

    if service_types:
        where_clauses.append(
            "JSON_OVERLAPS(service_types, %s)"
        )
        params.append(_json.dumps(service_types))

    if countries:
        placeholders = ", ".join(["%s"] * len(countries))
        where_clauses.append(f"country IN ({placeholders})")
        params.extend(countries)

    if source_portals:
        placeholders = ", ".join(["%s"] * len(source_portals))
        where_clauses.append(f"source_portal IN ({placeholders})")
        params.extend(source_portals)

    if min_fit_score > 0:
        where_clauses.append("fit_score >= %s")
        params.append(min_fit_score)

    if exclude_expired:
        where_clauses.append("(is_expired = 0 OR deadline IS NULL)")

    if exclude_duplicates:
        where_clauses.append("is_duplicate = 0")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    offset = (page - 1) * page_size

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # Total count
        cur.execute(f"SELECT COUNT(*) AS cnt FROM tenders {where_sql};", params)
        total = (cur.fetchone() or {}).get("cnt", 0)

        # Page of results
        cur.execute(
            f"SELECT * FROM tenders {where_sql} "
            f"ORDER BY {sort_by} {sort_order} "
            f"LIMIT %s OFFSET %s;",
            params + [page_size, offset],
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return {"results": rows, "total": total, "page": page, "page_size": page_size}

    except Error as e:
        _log.error("[db] search_tenders error: %s", e)
        return {"results": [], "total": 0, "page": page, "page_size": page_size}


# =============================================================================
# API STATS — single-connection aggregate query for the /stats endpoint
# =============================================================================

def get_api_stats() -> dict:
    """
    Return all data needed by GET /api/v1/stats and GET /api/v1/portals.

    Runs 3 independent query groups in parallel using ThreadPoolExecutor so
    total wall-clock time ≈ the slowest single query (~20ms), not their sum.

    Returns a dict with keys:
        total_tenders, tenders_last_24h, tenders_last_7_days,
        high_fit_count, duplicate_count, portal_breakdown, sector_breakdown
    Returns {} on any DB error (caller renders zeroed stats).
    """
    import json as _json
    from datetime import timedelta
    from concurrent.futures import ThreadPoolExecutor, as_completed

    now = datetime.now()

    # ── Query A: scalar counts + duplicate count ───────────────────────────────
    def _query_counts():
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*)                                           AS total_tenders,
                    SUM(CASE WHEN scraped_at >= %s THEN 1 ELSE 0 END) AS tenders_last_24h,
                    SUM(CASE WHEN scraped_at >= %s THEN 1 ELSE 0 END) AS tenders_last_7_days,
                    SUM(CASE WHEN fit_score  >= 80  THEN 1 ELSE 0 END) AS high_fit_count,
                    SUM(CASE WHEN is_duplicate = 1  THEN 1 ELSE 0 END) AS duplicate_count
                FROM tenders;
            """, (now - timedelta(hours=24), now - timedelta(days=7)))
            return cur.fetchone() or {}
        finally:
            cur.close(); conn.close()

    # ── Query B: per-portal breakdown ──────────────────────────────────────────
    def _query_portals():
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    source_portal,
                    COUNT(*)                                           AS total_tenders,
                    SUM(CASE WHEN scraped_at >= %s THEN 1 ELSE 0 END) AS new_last_7_days,
                    AVG(fit_score)                                     AS avg_fit_score,
                    SUM(CASE WHEN fit_score >= 80  THEN 1 ELSE 0 END) AS high_fit_count,
                    MAX(scraped_at)                                    AS last_scraped_at
                FROM tenders
                WHERE is_duplicate = 0
                GROUP BY source_portal
                ORDER BY total_tenders DESC;
            """, (now - timedelta(days=7),))
            return cur.fetchall() or []
        finally:
            cur.close(); conn.close()

    # ── Query C: sector breakdown (Python-side aggregation) ───────────────────
    def _query_sectors():
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT sectors FROM tenders
                WHERE  is_duplicate = 0 AND sectors IS NOT NULL
                LIMIT  10000;
            """)
            sector_counts: dict = {}
            for row in (cur.fetchall() or []):
                raw = row.get("sectors")
                if not raw:
                    continue
                if isinstance(raw, str):
                    try:
                        raw = _json.loads(raw)
                    except Exception:
                        continue
                for s in (raw or []):
                    if s:
                        sector_counts[s] = sector_counts.get(s, 0) + 1
            return sector_counts
        finally:
            cur.close(); conn.close()

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_counts  = pool.submit(_query_counts)
            f_portals = pool.submit(_query_portals)
            f_sectors = pool.submit(_query_sectors)

            counts      = f_counts.result()
            portal_rows = f_portals.result()
            sector_counts = f_sectors.result()

        return {
            "total_tenders":       int(counts.get("total_tenders",    0) or 0),
            "tenders_last_24h":    int(counts.get("tenders_last_24h", 0) or 0),
            "tenders_last_7_days": int(counts.get("tenders_last_7_days", 0) or 0),
            "high_fit_count":      int(counts.get("high_fit_count",   0) or 0),
            "duplicate_count":     int(counts.get("duplicate_count",  0) or 0),
            "portal_breakdown":    portal_rows,
            "sector_breakdown":    sector_counts,
        }

    except Exception as e:
        _log.error("[db] get_api_stats error: %s", e)
        return {}


# =============================================================================
# INTELLIGENCE-LAYER TENDER LIST — powers GET /api/v1/tenders
# =============================================================================

def get_intel_tenders(
    limit:            int            = 50,
    offset:           int            = 0,
    sector:           Optional[str]  = None,
    region:           Optional[str]  = None,
    min_priority:     int            = 0,
    source_site:      Optional[str]  = None,
    after_priority:   Optional[int]  = None,
    after_tender_id:  Optional[str]  = None,
) -> dict:
    """
    Return tenders enriched with structured intelligence for the dashboard API.

    Joins seen_tenders ⟵→ tender_structured_intel (both indexed on tender_id).
    Supports two pagination modes:

    1. Offset mode (default):  pass `offset` — O(n) at large offsets but simple.
    2. Keyset mode (preferred): pass `after_priority` + `after_tender_id` from
       the last row of the previous page.  O(log n) regardless of depth.

    Returns:
        {
            "results": [dict, ...],
            "total":   int,
            "limit":   int,
            "offset":  int,
            "next_cursor": {"after_priority": N, "after_tender_id": "..."} | None,
        }
    """
    where:  list = []
    params: list = []

    if sector:
        if isinstance(sector, str):
            sector = [sector]
        sector_clauses = [("si.sector LIKE %s" ) for _ in sector]
        params.extend(f"%{str(s).strip()}%" for s in sector)
        where.append("(" + " OR ".join(sector_clauses) + ")")

    if region:
        if isinstance(region, str):
            region = [region]
        region_clauses = ["si.region LIKE %s" for _ in region]
        params.extend(f"%{str(r).strip()}%" for r in region)
        where.append("(" + " OR ".join(region_clauses) + ")")

    if min_priority > 0:
        where.append("si.priority_score >= %s")
        params.append(int(min_priority))

    if source_site:
        if isinstance(source_site, str):
            source_site = [source_site]
        portal_clauses = ["st.source_site LIKE %s" for _ in source_site]
        params.extend(f"%{str(p).strip()}%" for p in source_site)
        where.append("(" + " OR ".join(portal_clauses) + ")")

    # Keyset cursor: rows with priority < after_priority,
    # or same priority but tender_id > after_tender_id (stable tie-break)
    use_keyset = after_priority is not None and after_tender_id is not None
    if use_keyset:
        where.append(
            "(si.priority_score < %s "
            " OR (si.priority_score = %s AND st.tender_id > %s))"
        )
        params.extend([int(after_priority), int(after_priority), str(after_tender_id)])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # LEFT JOINs throughout — seen_tenders is the authoritative source list.
    # Tenders not yet enriched by any intelligence module are still visible
    # (they appear with NULL scores rather than being silently excluded).
    base_sql = f"""
        FROM seen_tenders                  st
        LEFT JOIN tender_structured_intel  si ON st.tender_id = si.tender_id
        LEFT JOIN tenders                   t  ON st.tender_id = t.tender_id
        LEFT JOIN tender_intelligence       ti ON st.tender_id = ti.tender_id
        {where_sql}
    """

    select_cols = """
        st.tender_id,
        COALESCE(t.title_clean, st.title)       AS title,
        COALESCE(t.url,         st.url)          AS url,
        st.source_site,
        st.date_first_seen,
        -- Structured intel (rule-based fast scores)
        COALESCE(si.organization, t.organization, '')   AS organization,
        COALESCE(si.sector,       t.primary_sector, '') AS sector,
        COALESCE(si.consulting_type, '')                AS consulting_type,
        COALESCE(si.region,       t.country, 'global')  AS region,
        COALESCE(si.deadline_category, 'unknown')        AS deadline_category,
        COALESCE(si.relevance_score, 0)                  AS bid_fit_score,
        COALESCE(si.priority_score,  0)                  AS priority_score,
        COALESCE(si.competition_level, 'medium')         AS competition_level,
        COALESCE(si.opportunity_size,  'medium')         AS opportunity_size,
        COALESCE(si.complexity_score,  0)                AS complexity_score,
        si.opportunity_insight,
        si.enriched_at,
        -- Normalised tenders table (richer fields)
        t.country,
        t.deadline,
        t.deadline_raw,
        t.estimated_budget_usd,
        t.is_duplicate,
        t.is_expired,
        t.description,
        t.word_count,
        -- Deep enrichment fields
        t.deep_scope,
        t.deep_ai_summary,
        t.deep_document_links,
        t.deep_contract_duration,
        t.deep_budget_currency,
        t.deep_date_pre_bid,
        t.deep_date_qa_deadline,
        t.deep_date_contract_start,
        t.deep_eval_technical_weight,
        t.deep_eval_financial_weight,
        t.deep_min_years_experience,
        t.deep_min_similar_projects,
        t.amendment_count,
        t.last_amended_at,
        -- AI enrichment layer
        COALESCE(ti.fit_score, si.relevance_score, 0)   AS fit_score,
        ti.fit_explanation,
        ti.budget_usd
    """

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        # Total count — always uses the un-keyset-filtered base to return consistent total
        count_params = params
        if use_keyset:
            # Re-build without the keyset clause for an accurate total
            count_where = [w for w in where if "priority_score < %s" not in w
                           and "priority_score = %s" not in w]
            count_where_sql = ("WHERE " + " AND ".join(count_where)) if count_where else ""
            count_base = f"""
                FROM seen_tenders st
                JOIN tender_structured_intel si ON st.tender_id = si.tender_id
                {count_where_sql}
            """
            count_params = params[:-3]   # drop the 3 keyset params
        else:
            count_base = base_sql
        cur.execute(f"SELECT COUNT(*) AS cnt {count_base}", count_params)
        total = int((cur.fetchone() or {}).get("cnt", 0))

        # Paginated results
        page_params = params + [int(limit)]
        if use_keyset:
            cur.execute(
                f"SELECT {select_cols} {base_sql} "
                f"ORDER BY si.priority_score DESC, st.tender_id ASC "
                f"LIMIT %s",
                page_params,
            )
        else:
            cur.execute(
                f"SELECT {select_cols} {base_sql} "
                f"ORDER BY si.priority_score DESC, st.tender_id ASC "
                f"LIMIT %s OFFSET %s",
                params + [int(limit), int(offset)],
            )

        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        # Build next-page cursor from the last row
        next_cursor = None
        if rows and len(rows) == limit:
            last = rows[-1]
            next_cursor = {
                "after_priority":  int(last.get("priority_score") or 0),
                "after_tender_id": str(last.get("tender_id") or ""),
            }

        return {
            "results":     rows,
            "total":       total,
            "limit":       limit,
            "offset":      offset,
            "next_cursor": next_cursor,
        }
    except Error as e:
        _log.error("[db] get_intel_tenders error (JOIN failed: %s) — falling back to tenders table", e)
        return _get_tenders_fallback(limit=limit, offset=offset, sector=sector,
                                     region=region, source_site=source_site)


def _get_tenders_fallback(
    limit: int = 50,
    offset: int = 0,
    sector: Optional[str] = None,
    region: Optional[str] = None,
    source_site: Optional[str] = None,
) -> dict:
    """Direct query against the tenders table — avoids large JOINs that overflow Railway /tmp."""
    where: list = ["(is_expired = 0 OR deadline IS NULL)", "is_duplicate = 0"]
    params: list = []

    if sector:
        if isinstance(sector, str):
            sector = [sector]
        where.append("(" + " OR ".join("primary_sector LIKE %s" for _ in sector) + ")")
        params.extend(f"%{s}%" for s in sector)

    if region:
        if isinstance(region, str):
            region = [region]
        where.append("(" + " OR ".join("country LIKE %s" for _ in region) + ")")
        params.extend(f"%{r}%" for r in region)

    if source_site:
        if isinstance(source_site, str):
            source_site = [source_site]
        where.append("(" + " OR ".join("source_portal LIKE %s" for _ in source_site) + ")")
        params.extend(f"%{p}%" for p in source_site)

    where_sql = "WHERE " + " AND ".join(where)

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute(f"SELECT COUNT(*) AS cnt FROM tenders {where_sql}", params)
        total = int((cur.fetchone() or {}).get("cnt", 0))

        cur.execute(
            f"""SELECT
                tender_id,
                title_clean   AS title,
                url,
                source_portal AS source_site,
                scraped_at    AS date_first_seen,
                organization,
                primary_sector AS sector,
                ''            AS consulting_type,
                country       AS region,
                'unknown'     AS deadline_category,
                COALESCE(fit_score, 0) AS bid_fit_score,
                COALESCE(fit_score, 0) AS priority_score,
                'medium'      AS competition_level,
                'medium'      AS opportunity_size,
                0             AS complexity_score,
                NULL          AS opportunity_insight,
                NULL          AS enriched_at,
                country,
                deadline,
                deadline_raw,
                estimated_budget_usd,
                is_duplicate,
                is_expired,
                description,
                word_count,
                NULL AS deep_scope, NULL AS deep_ai_summary,
                NULL AS deep_document_links, NULL AS deep_contract_duration,
                NULL AS deep_budget_currency, NULL AS deep_date_pre_bid,
                NULL AS deep_date_qa_deadline, NULL AS deep_date_contract_start,
                NULL AS deep_eval_technical_weight, NULL AS deep_eval_financial_weight,
                NULL AS deep_min_years_experience, NULL AS deep_min_similar_projects,
                0 AS amendment_count, NULL AS last_amended_at,
                COALESCE(fit_score, 0) AS fit_score,
                NULL AS fit_explanation,
                NULL AS budget_usd
            FROM tenders {where_sql}
            ORDER BY scraped_at DESC
            LIMIT %s OFFSET %s""",
            params + [int(limit), int(offset)],
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return {"results": rows, "total": total, "limit": limit, "offset": offset, "next_cursor": None}
    except Error as e2:
        _log.error("[db] _get_tenders_fallback error: %s", e2)
        return {"results": [], "total": 0, "limit": limit, "offset": offset, "next_cursor": None}


# =============================================================================
# PIPELINE LIST — powers GET /api/v1/pipeline
# =============================================================================

def get_pipeline_entries(
    status_filter: Optional[str] = None,
    owner_filter:  Optional[str] = None,
    limit:         int           = 100,
    offset:        int           = 0,
) -> dict:
    """
    Return bid_pipeline rows joined with tender title + sector.

    Returns:
        {"results": [dict, ...], "total": int}
    """
    where: list = []
    params: list = []

    if status_filter:
        where.append("bp.status = %s")
        params.append(str(status_filter).strip())
    if owner_filter:
        where.append("bp.owner = %s")
        params.append(str(owner_filter).strip())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    base_sql = f"""
        FROM bid_pipeline bp
        LEFT JOIN seen_tenders        st ON bp.tender_id = st.tender_id
        LEFT JOIN tender_structured_intel si ON bp.tender_id = si.tender_id
        {where_sql}
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute(f"SELECT COUNT(*) AS cnt {base_sql}", params)
        total = int((cur.fetchone() or {}).get("cnt", 0))

        cur.execute(
            f"""
            SELECT
                bp.tender_id,
                bp.status,
                bp.owner,
                bp.notes,
                bp.proposal_deadline,
                bp.created_at,
                bp.updated_at,
                st.title,
                st.url,
                st.source_site,
                si.sector,
                si.region,
                si.organization,
                si.priority_score,
                si.opportunity_insight
            {base_sql}
            ORDER BY
                FIELD(bp.status,
                    'proposal_in_progress','shortlisted',
                    'discovered','submitted','won','lost'
                ),
                si.priority_score DESC
            LIMIT %s OFFSET %s
            """,
            params + [int(limit), int(offset)],
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return {"results": rows, "total": total}
    except Error as e:
        _log.error("[db] get_pipeline_entries error: %s", e)
        return {"results": [], "total": 0}


def update_pipeline_entry(
    tender_id:         str,
    status:            Optional[str] = None,
    owner:             Optional[str] = None,
    notes:             Optional[str] = None,
    proposal_deadline: Optional[str] = None,
) -> bool:
    """
    Partial-update a bid_pipeline row. Only provided (non-None) fields are written.
    Returns True on success, False if row not found or DB error.
    """
    if DRY_RUN:
        return True

    set_parts: list = []
    params:    list = []

    if status is not None:
        set_parts.append("status = %s")
        params.append(str(status).strip())
    if owner is not None:
        set_parts.append("owner = %s")
        params.append(str(owner).strip() or None)
    if notes is not None:
        set_parts.append("notes = %s")
        params.append(str(notes).strip() or None)
    if proposal_deadline is not None:
        set_parts.append("proposal_deadline = %s")
        params.append(proposal_deadline or None)

    if not set_parts:
        return True   # nothing to update

    params.append(str(tender_id).strip())
    sql = f"UPDATE bid_pipeline SET {', '.join(set_parts)} WHERE tender_id = %s"

    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        return affected > 0
    except Error as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log.error("[db] update_pipeline_entry error: %s", e)
        return False


# =============================================================================
# PIPELINE — single-row lookup by tender_id (used by transition validator)
# =============================================================================

def get_pipeline_entry(tender_id: str) -> Optional[dict]:
    """
    Fetch a single bid_pipeline row by tender_id (O(1) primary-key lookup).

    Returns dict with at minimum {"tender_id", "status"} or None if not found.
    This is the correct function for status-transition validation — never use
    get_pipeline_entries() which returns a page, not a specific tender.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                bp.tender_id, bp.status, bp.owner, bp.notes,
                bp.proposal_deadline, bp.created_at, bp.updated_at
            FROM bid_pipeline bp
            WHERE bp.tender_id = %s
            LIMIT 1;
            """,
            (str(tender_id).strip(),),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Error as e:
        _log.error("[db] get_pipeline_entry error for '%s': %s", tender_id, e)
        return None


# =============================================================================
# INTELLIGENCE — single-row lookup from tender_intelligence (for copilot)
# =============================================================================

def get_intelligence(tender_id: str) -> Optional[dict]:
    """
    Fetch the full AI-enrichment record from tender_intelligence for a tender.

    Returns dict or None. Used by copilot_engine to build rich LLM context
    (fit_explanation, top_reasons, red_flags, ai_summary, sector, budget_usd).
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM tender_intelligence WHERE tender_id = %s LIMIT 1;",
            (str(tender_id).strip(),),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row
    except Error as e:
        _log.error("[db] get_intelligence error for '%s': %s", tender_id, e)
        return None


# =============================================================================
# CROSS-SOURCES — fetch merged portal sources for tender detail page
# =============================================================================

def get_cross_sources_db(tender_id: str) -> list:
    """
    Fetch cross-portal source entries for a tender from tender_cross_sources.
    Returns list of dicts. Used by the detail page API to surface duplicate portal links.
    Non-fatal on missing table — returns [] if table doesn't exist yet.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                COALESCE(source_portal, portal) AS source_portal,
                COALESCE(source_url, url) AS source_url,
                unique_fields,
                COALESCE(detected_at, added_at) AS detected_at
            FROM   tender_cross_sources
            WHERE  tender_id = %s
            ORDER BY COALESCE(detected_at, added_at) DESC;
            """,
            (str(tender_id).strip(),),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return rows
    except Error:
        return []   # table may not exist yet — non-fatal


# =============================================================================
# DASHBOARD SUMMARY — powers GET /api/v1/summary
# =============================================================================

def get_dashboard_summary() -> dict:
    """
    Parallel aggregate queries for GET /api/v1/summary.

    Runs 5 independent SQL aggregations concurrently via ThreadPoolExecutor,
    each on its own connection, then assembles results. Reduces wall-clock
    latency from ~250ms (sequential) to ~60ms (parallel) on a warm DB.

    Returns:
        {
            "total_tenders":       int,
            "high_priority_count": int,   # priority_score >= 70
            "pipeline_counts":     {status: count, ...},
            "top_sectors":         [(sector, count), ...],
            "top_organizations":   [(org, count), ...],
            "portals_active":      int,
            "deadline_breakdown":  dict,
        }
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # ── Individual query functions (each opens/closes its own connection) ─────

    def _fetch_totals() -> dict:
        conn = get_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT
                    COUNT(*)                                               AS total_tenders,
                    SUM(CASE WHEN priority_score >= 70 THEN 1 ELSE 0 END) AS high_priority_count,
                    COUNT(DISTINCT st.source_site)                         AS portals_active
                FROM tender_structured_intel si
                JOIN seen_tenders st ON si.tender_id = st.tender_id
            """)
            row = cur.fetchone() or {}
            cur.close()
            return row
        finally:
            conn.close()

    def _fetch_pipeline() -> dict:
        conn = get_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT status, COUNT(*) AS cnt
                FROM bid_pipeline
                GROUP BY status
            """)
            result = {r["status"]: int(r["cnt"]) for r in (cur.fetchall() or [])}
            cur.close()
            return result
        finally:
            conn.close()

    def _fetch_top_sectors() -> list:
        conn = get_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT sector, COUNT(*) AS cnt
                FROM tender_structured_intel
                WHERE sector IS NOT NULL AND sector NOT IN ('unknown', '')
                GROUP BY sector
                ORDER BY cnt DESC
                LIMIT 10
            """)
            result = [(r["sector"], int(r["cnt"])) for r in (cur.fetchall() or [])]
            cur.close()
            return result
        finally:
            conn.close()

    def _fetch_top_orgs() -> list:
        conn = get_connection()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT organization, COUNT(*) AS cnt
                FROM tender_structured_intel
                WHERE organization IS NOT NULL AND organization NOT IN ('unknown', '')
                GROUP BY organization
                ORDER BY cnt DESC
                LIMIT 10
            """)
            result = [(r["organization"], int(r["cnt"])) for r in (cur.fetchall() or [])]
            cur.close()
            return result
        finally:
            conn.close()

    def _fetch_deadline_breakdown() -> dict:
        conn = get_connection()
        try:
            cur = conn.cursor(dictionary=True)
            _WITH_TIERS = """
                SELECT
                    CASE
                        WHEN deadline_extracted IS NULL           THEN 'unknown'
                        WHEN deadline_extracted < CURDATE()      THEN 'expired'
                        WHEN DATEDIFF(deadline_extracted, CURDATE()) <= 7  THEN 'closing_soon'
                        WHEN DATEDIFF(deadline_extracted, CURDATE()) <= 21 THEN 'needs_action'
                        ELSE 'plan_ahead'
                    END AS bucket,
                    COUNT(*) AS total,
                    SUM(CASE WHEN COALESCE(decision_tag,'IGNORE') = 'BID_NOW'
                             THEN 1 ELSE 0 END) AS bid_now_count,
                    SUM(CASE WHEN COALESCE(decision_tag,'IGNORE') = 'STRONG_CONSIDER'
                             THEN 1 ELSE 0 END) AS strong_count
                FROM tender_structured_intel
                GROUP BY bucket
            """
            _NO_TIERS = """
                SELECT
                    CASE
                        WHEN deadline_extracted IS NULL           THEN 'unknown'
                        WHEN deadline_extracted < CURDATE()      THEN 'expired'
                        WHEN DATEDIFF(deadline_extracted, CURDATE()) <= 7  THEN 'closing_soon'
                        WHEN DATEDIFF(deadline_extracted, CURDATE()) <= 21 THEN 'needs_action'
                        ELSE 'plan_ahead'
                    END AS bucket,
                    COUNT(*) AS total,
                    0 AS bid_now_count,
                    0 AS strong_count
                FROM tender_structured_intel
                GROUP BY bucket
            """
            try:
                cur.execute(_WITH_TIERS)
                rows = cur.fetchall() or []
            except Exception:
                cur.execute(_NO_TIERS)
                rows = cur.fetchall() or []

            breakdown: dict = {}
            active_total = 0
            for r in rows:
                bucket = r.get("bucket", "unknown")
                total  = int(r.get("total", 0) or 0)
                breakdown[bucket] = {
                    "total":   total,
                    "bid_now": int(r.get("bid_now_count", 0) or 0),
                    "strong":  int(r.get("strong_count",  0) or 0),
                }
                if bucket in ("closing_soon", "needs_action", "plan_ahead"):
                    active_total += total
            breakdown["active_total"] = active_total
            cur.close()
            return breakdown
        finally:
            conn.close()

    # ── Run all 5 queries in parallel ─────────────────────────────────────────
    tasks = {
        "totals":            _fetch_totals,
        "pipeline":          _fetch_pipeline,
        "top_sectors":       _fetch_top_sectors,
        "top_orgs":          _fetch_top_orgs,
        "deadline":          _fetch_deadline_breakdown,
    }
    results: dict = {}
    errors:  list = []

    try:
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fn): key for key, fn in tasks.items()}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as exc:
                    _log.warning("[db] get_dashboard_summary/%s error: %s", key, exc)
                    errors.append(key)

        totals            = results.get("totals", {})
        pipeline_counts   = results.get("pipeline", {})
        top_sectors       = results.get("top_sectors", [])
        top_organizations = results.get("top_orgs", [])
        deadline_breakdown = results.get("deadline", {})

        return {
            "total_tenders":       int(totals.get("total_tenders",       0) or 0),
            "high_priority_count": int(totals.get("high_priority_count", 0) or 0),
            "portals_active":      int(totals.get("portals_active",      0) or 0),
            "pipeline_counts":     pipeline_counts,
            "top_sectors":         top_sectors,
            "top_organizations":   top_organizations,
            "deadline_breakdown":  deadline_breakdown,
        }

    except Exception as e:
        _log.error("[db] get_dashboard_summary error: %s", e)
        return {
            "total_tenders":       0,
            "high_priority_count": 0,
            "portals_active":      0,
            "pipeline_counts":     {},
            "top_sectors":         [],
            "top_organizations":   [],
            "deadline_breakdown":  {},
        }


# =============================================================================
# PIPELINE OUTCOME — record win/loss/no-bid for ML feedback loop
# =============================================================================

_VALID_OUTCOMES   = frozenset({"won", "lost", "no_submission", "pending"})
_VALID_BID_DECS   = frozenset({"bid", "no_bid", "review_later"})

# Allowed forward-only status transitions (prevents Kanban data corruption).
# Keys = current status, values = set of statuses it may move to.
_STATUS_TRANSITIONS: dict = {
    "discovered":           {"shortlisted", "lost"},
    "shortlisted":          {"proposal_in_progress", "discovered", "lost"},
    "proposal_in_progress": {"submitted", "shortlisted", "lost"},
    "submitted":            {"won", "lost"},
    "won":                  set(),        # terminal
    "lost":                 set(),        # terminal
}


def record_pipeline_outcome(
    tender_id:    str,
    outcome:      str,
    bid_decision: str = "bid",
) -> bool:
    """
    Record the final outcome of a pipeline opportunity.

    Validates outcome/bid_decision consistency, updates bid_pipeline status
    to 'won' or 'lost', and writes outcome metadata columns.

    Also ensures the `outcome` and `bid_decision` columns exist (idempotent
    ALTER TABLE) so the function never hard-fails on a fresh schema.

    Returns True on success, False on error.
    """
    if DRY_RUN:
        return True

    outcome      = (outcome      or "").strip().lower()
    bid_decision = (bid_decision or "bid").strip().lower()

    if outcome not in _VALID_OUTCOMES:
        _log.error("[db] record_pipeline_outcome: invalid outcome '%s'", outcome)
        return False
    if bid_decision not in _VALID_BID_DECS:
        _log.error("[db] record_pipeline_outcome: invalid bid_decision '%s'", bid_decision)
        return False
    if outcome in ("won", "lost") and bid_decision != "bid":
        _log.error("[db] record_pipeline_outcome: outcome '%s' requires bid_decision='bid'", outcome)
        return False

    # Map outcome → pipeline status
    status_map = {"won": "won", "lost": "lost",
                  "no_submission": "lost", "pending": None}
    new_status = status_map.get(outcome)

    try:
        conn = get_connection()
        cur  = conn.cursor()

        # Ensure outcome + bid_decision columns exist (idempotent)
        for col_def in [
            "outcome      VARCHAR(20)  DEFAULT NULL",
            "bid_decision VARCHAR(20)  DEFAULT NULL",
        ]:
            col_name = col_def.split()[0]
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'bid_pipeline'
                  AND COLUMN_NAME  = %s;
            """, (col_name,))
            if not (cur.fetchone() or (0,))[0]:
                cur.execute(
                    f"ALTER TABLE bid_pipeline ADD COLUMN {col_def};"
                )
                conn.commit()
                _log.info("[db] Added column %s to bid_pipeline.", col_name)

        # Build SET clauses
        set_parts = ["outcome = %s", "bid_decision = %s"]
        params    = [outcome, bid_decision]
        if new_status:
            set_parts.append("status = %s")
            params.append(new_status)

        params.append(str(tender_id).strip())
        cur.execute(
            f"UPDATE bid_pipeline SET {', '.join(set_parts)} WHERE tender_id = %s",
            params,
        )
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()

        if affected == 0:
            _log.warning("[db] record_pipeline_outcome: tender '%s' not in pipeline", tender_id)
        return affected > 0

    except Error as e:
        try:
            conn.rollback()  # type: ignore[possibly-undefined]
        except Exception:
            pass
        _log.error("[db] record_pipeline_outcome error: %s", e)
        return False


def validate_status_transition(current: str, new: str) -> bool:
    """
    Return True if transitioning from `current` to `new` is permitted.
    Unknown statuses are allowed through (safe default for new portals).
    """
    allowed = _STATUS_TRANSITIONS.get(current)
    if allowed is None:
        return True          # unknown current status — allow
    if new == current:
        return True          # no-op is always fine
    return new in allowed


# =============================================================================
# COPILOT — rich tender context for LLM bid recommendations
# =============================================================================

def get_tender_for_copilot(tender_id: str) -> Optional[dict]:
    """
    Fetch a full tender context dict for the copilot engine.

    Combines tenders (normalised) with tender_structured_intel for all
    enrichment fields (priority_score, bid_fit_score, competition_level,
    opportunity_size, opportunity_insight, sector, region, etc.).

    Falls back to tenders-only row if no structured intel exists yet.

    Returns:
        dict with all relevant fields, or None if tender_id not found.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT
                t.tender_id,
                t.title,
                t.title_clean,
                t.organization,
                t.country,
                t.source_portal,
                t.url,
                t.deadline,
                t.deadline_raw,
                t.description,
                t.word_count,
                t.fit_score,
                t.semantic_score,
                t.keyword_score,
                t.fit_explanation,
                t.top_reasons,
                t.red_flags,
                t.sectors,
                t.service_types,
                t.primary_sector,
                t.estimated_budget_usd,
                t.is_expired,
                t.is_duplicate,
                si.organization         AS si_organization,
                si.sector               AS sector,
                si.consulting_type,
                si.region,
                si.deadline_category,
                si.relevance_score      AS bid_fit_score,
                si.priority_score,
                si.competition_level,
                si.opportunity_size,
                si.opportunity_insight
            FROM tenders t
            LEFT JOIN seen_tenders        st ON t.tender_id = st.tender_id
            LEFT JOIN tender_structured_intel si ON t.tender_id = si.tender_id
            WHERE t.tender_id = %s
            LIMIT 1;
        """, (str(tender_id),))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        # Prefer structured intel org over tenders table (richer normalisation)
        if row.get("si_organization"):
            row["organization"] = row["si_organization"]
        row.pop("si_organization", None)

        # Deserialise JSON columns from the tenders table
        import json as _json
        for col in ("top_reasons", "red_flags", "sectors", "service_types"):
            val = row.get(col)
            if isinstance(val, str):
                try:
                    row[col] = _json.loads(val)
                except Exception:
                    row[col] = []
            elif val is None:
                row[col] = []

        return row

    except Error as e:
        _log.error("[db] get_tender_for_copilot error: %s", e)
        return None


# =============================================================================
# WORLD BANK EARLY PIPELINE SCHEMA
# =============================================================================

def init_wb_early_schema() -> None:
    """
    Create world_bank_early_pipeline and migrate existing installs to v2 schema.

    v1 columns (created by earlier version):
        project_id, project_name, country, region, sector, approval_date,
        status, procurement_plan_flag, consulting_signal, early_signal_score,
        score_reason, project_url, description, first_seen, last_updated, notified

    v2 NEW columns (added here if missing):
        project_stage      — pipeline | approved | active
        last_signal_score  — adjusted_score from the previous run (change tracking)
        content_hash       — MD5 of (consulting_signal|sector|description[:500])
        firm_fit_score     — net boost/penalty from firm_profile.json
        firm_fit_reason    — human-readable fit breakdown
        adjusted_score     — early_signal_score + firm_fit_score, capped 0-100
        start_estimate     — approval_date + 90d (DATE)
        end_estimate       — approval_date + 180d (DATE)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # ── CREATE base table (v1 shape) ──────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS world_bank_early_pipeline (
                id                    INT AUTO_INCREMENT PRIMARY KEY,
                project_id            VARCHAR(30)  UNIQUE NOT NULL,
                project_name          TEXT,
                country               VARCHAR(100),
                region                VARCHAR(100),
                sector                VARCHAR(1000),
                approval_date         DATE,
                status                VARCHAR(50),
                procurement_plan_flag TINYINT(1)   NOT NULL DEFAULT 0,
                consulting_signal     VARCHAR(1000),
                early_signal_score    SMALLINT     NOT NULL DEFAULT 0,
                score_reason          VARCHAR(1000),
                project_url           TEXT,
                description           TEXT,
                first_seen            DATETIME,
                last_updated          DATETIME,
                notified              TINYINT(1)   NOT NULL DEFAULT 0,
                INDEX idx_score       (early_signal_score),
                INDEX idx_notified    (notified),
                INDEX idx_approval    (approval_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()

        # ── ADD v2 columns if missing (safe on existing installs) ─────────────
        _v2_columns = [
            ("project_stage",     "VARCHAR(20)  NOT NULL DEFAULT 'active'"),
            ("last_signal_score", "SMALLINT     NOT NULL DEFAULT 0"),
            ("content_hash",      "VARCHAR(64)"),
            ("firm_fit_score",    "SMALLINT     NOT NULL DEFAULT 0"),
            ("firm_fit_reason",   "VARCHAR(1000)"),
            ("adjusted_score",    "SMALLINT     NOT NULL DEFAULT 0"),
            ("start_estimate",    "DATE"),
            ("end_estimate",      "DATE"),
        ]
        for col_name, col_def in _v2_columns:
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME   = 'world_bank_early_pipeline'
                   AND COLUMN_NAME  = %s
            """, (col_name,))
            exists = (cur.fetchone() or (0,))[0] > 0
            if not exists:
                cur.execute(
                    f"ALTER TABLE world_bank_early_pipeline "
                    f"ADD COLUMN {col_name} {col_def};"
                )
                conn.commit()
                _log.info("[db] world_bank_early_pipeline: added column '%s'", col_name)

        # ── Seed adjusted_score for existing rows that have none ─────────────
        cur.execute("""
            UPDATE world_bank_early_pipeline
               SET adjusted_score = early_signal_score
             WHERE adjusted_score = 0 AND early_signal_score > 0
        """)
        conn.commit()

        cur.close()
        conn.close()
        _log.info("[db] Table 'world_bank_early_pipeline' ready (v2).")
    except Error as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] world_bank_early_pipeline init warning (non-fatal): %s", e)


# =============================================================================
# OPPORTUNITY SIGNALS SCHEMA
# =============================================================================

def init_opportunity_signals_schema() -> None:
    """
    Create the generic opportunity_signals table for pre-tender intelligence.

    Signals are intentionally stored separately from tenders so signal-stage
    opportunities do not affect the live tender scoring or export flow.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS opportunity_signals (
                id                        INT AUTO_INCREMENT PRIMARY KEY,
                signal_uid                VARCHAR(255) NOT NULL UNIQUE,
                source                    VARCHAR(120) NOT NULL,
                source_record_id          VARCHAR(255),
                title                     TEXT,
                organization              VARCHAR(300),
                geography                 VARCHAR(200),
                sector                    VARCHAR(500),
                summary                   MEDIUMTEXT,
                signal_stage              VARCHAR(30)  NOT NULL DEFAULT 'EARLY_SIGNAL',
                confidence_score          SMALLINT     NOT NULL DEFAULT 0,
                consulting_signal         TINYINT(1)   NOT NULL DEFAULT 0,
                consulting_signal_reason  VARCHAR(1000),
                url                       TEXT,
                published_date            DATE,
                captured_at               DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at                DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                recommended_action        VARCHAR(100),
                raw_stage                 VARCHAR(100),
                procurement_signal        TINYINT(1)   NOT NULL DEFAULT 0,
                url_hash                  VARCHAR(64),
                title_hash                VARCHAR(64),
                content_hash              VARCHAR(64),
                metadata_json             JSON,
                INDEX idx_signal_source   (source),
                INDEX idx_signal_stage    (signal_stage),
                INDEX idx_signal_consult  (consulting_signal),
                INDEX idx_signal_score    (confidence_score),
                INDEX idx_signal_pub      (published_date),
                INDEX idx_signal_urlhash  (url_hash),
                INDEX idx_signal_titlehash (title_hash)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        conn.commit()

        for col_name, col_def in (
            ("url_hash", "VARCHAR(64)"),
            ("title_hash", "VARCHAR(64)"),
        ):
            cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME   = 'opportunity_signals'
                   AND COLUMN_NAME  = %s
            """, (col_name,))
            exists = (cur.fetchone() or (0,))[0] > 0
            if not exists:
                cur.execute(f"ALTER TABLE opportunity_signals ADD COLUMN {col_name} {col_def};")
                conn.commit()
                _log.info("[db] opportunity_signals: added column '%s'", col_name)
        cur.close()
        conn.close()
        _log.info("[db] Table 'opportunity_signals' ready.")
    except Error as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        _log.warning("[db] opportunity_signals init warning (non-fatal): %s", e)


# ── Test ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _log.info("Testing DB connection...")
    try:
        init_db()
        # Quick round-trip test
        test_id = "TEST_TENDER_001"
        assert check_if_new(test_id),   "Should be new"
        mark_as_seen(test_id, "Test tender", "test", "http://example.com")
        assert not check_if_new(test_id), "Should now be seen"
        # Clean up test row
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("DELETE FROM seen_tenders WHERE tender_id = %s;", (test_id,))
        conn.commit(); cur.close(); conn.close()
        _log.info("[db] All tests passed. DB is working correctly.")
    except Exception as e:
        _log.error("[db] Test FAILED: %s", e)
