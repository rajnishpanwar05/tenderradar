# =============================================================================
# core/scraper_monitor.py — Shared Monitoring for All TenderRadar Scrapers
#
# Provides ScraperMonitor (one instance per scraper portal):
#   • Per-portal run history   (logs/run_history/{portal}.json, last 30 runs)
#   • Quality monitoring       (pass-rate alert when < 30%)
#   • Structure/schema change  (0-row alert when historical avg > 20)
#   • Snapshot saving          (logs/snapshots/{portal}/YYYY-MM-DD.json)
#
# Usage in portal scrapers:
#   monitor = ScraperMonitor("world_bank")
#   monitor.run_quality_monitoring(scraped_total, accepted_rows, reasons, raw)
#   monitor.check_structure_change(accepted_count, raw_data)
# =============================================================================

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any


# ── Base directory ─────────────────────────────────────────────────────────────
_LOG_ROOT = Path(__file__).resolve().parent.parent / "logs"


def _portal_slug(name: str) -> str:
    """Normalise portal name to a safe filename slug."""
    return re.sub(r"[^a-z0-9]", "_", name.lower().strip()).strip("_")


class ScraperMonitor:
    """
    Per-portal monitoring instance.  Create once per scraper run:

        monitor = ScraperMonitor("world_bank")
        monitor.run_quality_monitoring(...)
        monitor.check_structure_change(...)
    """

    # Alert thresholds (class-level — override per instance if needed)
    QUALITY_DROP_THRESHOLD:     float = 0.30   # < 30 % pass rate triggers alert
    QUALITY_DROP_MIN_SCRAPED:   int   = 5      # don't alert on tiny batches
    STRUCTURE_CHANGE_AVG_FLOOR: int   = 20     # historical avg must exceed this
    HISTORY_WINDOW:             int   = 30     # keep last N run records

    def __init__(self, portal_name: str):
        self.portal_name = portal_name
        self._slug       = _portal_slug(portal_name)
        self._history_file = (
            _LOG_ROOT / "run_history" / f"{self._slug}.json"
        )
        self._snapshot_dir = _LOG_ROOT / "snapshots" / self._slug

    # =========================================================================
    # 1 — Run Quality Monitoring
    # =========================================================================

    def run_quality_monitoring(
        self,
        scraped_total:  int,
        accepted_rows:  list[dict],
        reject_reasons: list[str],
        raw_data:       Any = None,
    ) -> None:
        """
        Log quality metrics and alert when pass-rate < QUALITY_DROP_THRESHOLD.

        Persists per-run stats to history file.
        """
        if scraped_total == 0:
            print(f"[{self.portal_name}] Monitor: 0 scraped rows — skipping quality check")
            return

        accepted_count = len(accepted_rows)
        pass_rate      = accepted_count / scraped_total

        scores     = [int(r.get("quality_score") or r.get("Quality Score") or 0)
                      for r in accepted_rows]
        avg_score  = sum(scores) / len(scores) if scores else 0.0

        # Count rejection reasons
        reason_counts: dict[str, int] = {}
        for r in reject_reasons:
            reason_counts[r] = reason_counts.get(r, 0) + 1
        top_reasons = sorted(reason_counts.items(), key=lambda x: -x[1])[:3]

        print(f"\n[{self.portal_name}] ── Quality Monitor "
              f"{'─' * (40 - len(self.portal_name))}")
        print(f"[{self.portal_name}]    Scraped (before filter) : {scraped_total}")
        print(f"[{self.portal_name}]    Accepted (after filter) : "
              f"{accepted_count} ({pass_rate:.0%})")
        print(f"[{self.portal_name}]    Avg quality score       : {avg_score:.1f} / 100")
        if top_reasons:
            reasons_str = " | ".join(f"{r} ×{n}" for r, n in top_reasons)
            print(f"[{self.portal_name}]    Top rejection reasons   : {reasons_str}")
        print(f"[{self.portal_name}] {'─' * 55}")

        # Persist
        self._append_history({
            "date":        date.today().isoformat(),
            "scraped":     scraped_total,
            "accepted":    accepted_count,
            "pass_rate":   round(pass_rate, 3),
            "avg_quality": round(avg_score, 1),
        })

        # Alert
        if (scraped_total >= self.QUALITY_DROP_MIN_SCRAPED
                and pass_rate < self.QUALITY_DROP_THRESHOLD):
            print(f"\n[{self.portal_name}] ⚠️  QUALITY DROP DETECTED "
                  f"— POSSIBLE PARSER ISSUE")
            print(f"[{self.portal_name}]    Pass rate : {pass_rate:.0%}  "
                  f"(threshold: {self.QUALITY_DROP_THRESHOLD:.0%})")
            print(f"[{self.portal_name}]    Scraped   : {scraped_total}  |  "
                  f"Accepted : {accepted_count}")
            self.save_snapshot(raw_data, label="quality_drop")

    # =========================================================================
    # 2 — Structure / Schema Change Detection
    # =========================================================================

    def check_structure_change(
        self,
        accepted_count: int,
        raw_data:       Any = None,
    ) -> bool:
        """
        Alert when accepted_count == 0 AND historical avg > STRUCTURE_CHANGE_AVG_FLOOR.
        Also updates today's row-count in history.

        Returns True if a structure change was detected.
        """
        history  = self._load_history()
        today    = date.today().isoformat()
        past     = [h["accepted"] for h in history
                    if h.get("date") != today and "accepted" in h]
        hist_avg = sum(past) / len(past) if past else 0.0

        # Update today's entry (may have been written by quality monitoring already)
        for entry in history:
            if entry.get("date") == today:
                entry["accepted"] = accepted_count
                break
        else:
            history.append({
                "date": today, "accepted": accepted_count,
                "scraped": accepted_count,
            })
        self._save_history(history)

        if accepted_count == 0 and hist_avg > self.STRUCTURE_CHANGE_AVG_FLOOR:
            print(f"\n[{self.portal_name}] ⚠️  STRUCTURE CHANGE DETECTED")
            print(f"[{self.portal_name}]    Rows this run   : 0")
            print(f"[{self.portal_name}]    Historical avg  : "
                  f"{hist_avg:.1f} (over {len(past)} runs)")
            print(f"[{self.portal_name}]    Likely: site redesign or API format change")
            self.save_snapshot(raw_data, label="structure_change")
            return True
        return False

    def check_schema_change(
        self,
        violation_rate: float,
        raw_data:       Any = None,
        checked_fields: list[str] | None = None,
    ) -> None:
        """
        Log a SCHEMA CHANGE DETECTED alert when field violation rate > 20%.
        Called by portals that perform deep schema validation.
        """
        if violation_rate > 0.20:
            checked = ", ".join(checked_fields or [])
            print(f"\n[{self.portal_name}] ⚠️  SCHEMA CHANGE DETECTED")
            print(f"[{self.portal_name}]    Violation rate  : {violation_rate:.0%}")
            print(f"[{self.portal_name}]    Fields checked  : {checked}")
            self.save_snapshot(raw_data, label="schema_change")

    # =========================================================================
    # 3 — Snapshot Saving
    # =========================================================================

    def save_snapshot(self, data: Any, label: str = "snapshot") -> Path | None:
        """
        Persist raw data to disk for post-mortem inspection.
        Returns the file path, or None if saving failed.
        """
        if data is None:
            return None
        try:
            self._snapshot_dir.mkdir(parents=True, exist_ok=True)
            fname = (self._snapshot_dir
                     / f"{label}_{date.today().isoformat()}.json")
            with open(fname, "w") as fh:
                json.dump(data, fh, indent=2, default=str)
            print(f"[{self.portal_name}]    Snapshot saved  : {fname}")
            return fname
        except Exception as exc:
            print(f"[{self.portal_name}]    Snapshot save failed: {exc}")
            return None

    # =========================================================================
    # 4 — History persistence
    # =========================================================================

    def _load_history(self) -> list[dict]:
        try:
            if self._history_file.exists():
                with open(self._history_file) as fh:
                    return json.load(fh)
        except Exception:
            pass
        return []

    def _save_history(self, history: list[dict]) -> None:
        try:
            self._history_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._history_file, "w") as fh:
                json.dump(history[-self.HISTORY_WINDOW:], fh, indent=2)
        except Exception as exc:
            print(f"[{self.portal_name}] Could not save run history: {exc}")

    def _append_history(self, entry: dict) -> None:
        history = self._load_history()
        today   = date.today().isoformat()
        # Replace today's entry if already exists
        history = [h for h in history if h.get("date") != today]
        history.append(entry)
        self._save_history(history)

    # =========================================================================
    # 5 — Historical averages (utility for portals)
    # =========================================================================

    def historical_avg_accepted(self, exclude_today: bool = True) -> float:
        """Return the historical average of accepted rows (excluding today)."""
        today   = date.today().isoformat()
        history = self._load_history()
        past    = [
            h["accepted"] for h in history
            if "accepted" in h and (not exclude_today or h.get("date") != today)
        ]
        return sum(past) / len(past) if past else 0.0

    def last_n_pass_rates(self, n: int = 5) -> list[float]:
        """Return the last N recorded pass_rate values (most recent last)."""
        history = self._load_history()
        rates   = [h["pass_rate"] for h in history if "pass_rate" in h]
        return rates[-n:]
