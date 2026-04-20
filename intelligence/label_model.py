"""
Shadow label model for TenderRadar — Phase 1 supervised ML.

Predicts IDCG fit label:
  2 = Relevant
  1 = Borderline
  0 = Irrelevant

Design principles:
  - Separates TECHNICAL FIT from DELIVERY FEASIBILITY
    (geography/network weakness → Borderline signal, not Irrelevant)
  - Signal-first vs package-first distinction
    (title length + deep text proxy for RFP maturity)
  - Hard gates (goods/works, IC roles) are preserved upstream — this
    model scores consulting-eligible tenders only; gate logic stays in
    opportunity_engine.py
  - Trained on human-reviewed labels from master workbook
  - Loaded as a lazy singleton; returns neutral score (50) if not available
  - Shadow model: blended conservatively (35% weight in production score)

Artifact: artifacts/label_model.joblib
Meta:     artifacts/label_model_meta.json
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

_log = logging.getLogger("tenderradar.label_model")

try:
    import joblib as _joblib
except ImportError:
    _joblib = None

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE = Path(__file__).resolve().parent.parent
_MODEL_PATH = _BASE / "artifacts" / "label_model.joblib"
_META_PATH  = _BASE / "artifacts" / "label_model_meta.json"

# ── Label encoding ─────────────────────────────────────────────────────────────
LABEL_MAP = {
    "Relevant":    2,
    "Borderline":  1,
    "Irrelevant":  0,
    "Not Relevant": 0,
}
LABEL_INV = {2: "Relevant", 1: "Borderline", 0: "Irrelevant"}

# ── Feature patterns derived from CAPSTAT / PDS ────────────────────────────────

# Service-type patterns (IDCG's core delivery areas)
_SVC_EVAL   = [r"\bevaluat", r"\bassessment\b", r"\bimpact\b", r"\breview\b"]
_SVC_MNE    = [r"\bM&E\b", r"\bMEL\b", r"\bmonitoring.*evaluation\b",
               r"\bmonitoring\b", r"\bmne\b"]
_SVC_BASE   = [r"\bbaseline\b", r"\bmidline\b", r"\bendline\b"]
_SVC_TPM    = [r"\bTPM\b", r"\bIVA\b", r"\bthird.?party\b",
               r"\bindependent verification\b", r"\bverif"]
_SVC_TA     = [r"\btechnical assistance\b", r"\badvisory\b", r"\bTA\b"]
_SVC_CAP    = [r"\bcapacity building\b", r"\btraining\b", r"\binstitutional strength"]
_SVC_RSRCH  = [r"\bresearch\b", r"\bstudy\b", r"\bsurvey\b",
               r"\bdiagnostic\b", r"\bfeasibility\b", r"\bdata collect"]

# Hard-gate signals (upstream; used here for feature weight only)
_IC_ROLE    = [r"\bindividual consultant\b", r"\bnational consultant\b",
               r"\binternational consultant\b", r"\bSTC\b",
               r"\bshort.?term consultant\b", r"\bindividual contractor\b"]
_GOODS_WRK  = [r"\bsupply of\b", r"\bprocurement of goods\b", r"\bcivil works?\b",
               r"\bconstruction\b", r"\bequipment\b", r"\bmanpower supply\b",
               r"\bhousekeeping\b", r"\bsoftware development\b"]

# Sector signals (from IDCG CAPSTAT history)
_SEC_EDU    = [r"\beducation\b", r"\bschool\b", r"\blearning\b",
               r"\bliteracy\b", r"\bskills?\b", r"\bvocational\b"]
_SEC_AGR    = [r"\bagriculture\b", r"\bfarmer\b", r"\bfarming\b",
               r"\blivelihoods?\b", r"\brural\b", r"\bcrop\b", r"\bfood security\b"]
_SEC_ENV    = [r"\benvironment\b", r"\bforestry\b", r"\bclimate\b",
               r"\blandscape\b", r"\bbiodiversity\b", r"\becosystem\b", r"\bnature\b"]
_SEC_HEALTH = [r"\bhealth\b", r"\bnutrition\b", r"\bsanitation\b",
               r"\bwash\b", r"\bmaternal\b", r"\bchild health\b"]
_SEC_ENERGY = [r"\benergy\b", r"\brenewable\b", r"\bsolar\b",
               r"\bpower\b", r"\belectricity\b"]
_SEC_GOV    = [r"\bgovernance\b", r"\bpolicy\b", r"\bpublic sector\b",
               r"\binstitutional\b", r"\bMSME\b", r"\bdecentrali"]

# Client tiers (from PDS history)
_CLIENT_T1 = {
    "World Bank", "UNDP", "GIZ", "UNICEF", "FAO", "WFP", "ADB", "AfDB",
    "IFC", "KfW", "AFD", "JICA", "USAID", "FCDO", "DFID",
    "European Commission", "European Union", "IFAD", "ILO", "MCC",
}
_CLIENT_T2_PAT = [
    r"\bIUCN\b", r"\bWinrock\b", r"\bTNC\b", r"\bRoom to Read\b",
    r"\bSave the Children\b", r"\bTata Trust\b", r"\bHans Foundation\b",
    r"\bMicroSave\b", r"\bMSC\b", r"\bCUTS\b", r"\bCLASP\b", r"\bWRI\b",
    r"\bSELCO\b", r"\bOxfam\b", r"\bCARE\b", r"\bPlan International\b",
    r"\bBritish Council\b", r"\bReliance Foundation\b", r"\bLeadership for Equity\b",
    r"\bGenesis Analytics\b", r"\bTanager\b", r"\bI4DI\b",
]
_NATL_GOV   = [r"\bGovernment of\b", r"\bMinistry\b", r"\bDepartment\b",
               r"\bState\b.*\bGovernment\b"]

# Geographies: TECHNICAL FIT vs DELIVERY FEASIBILITY separation
# Weak-presence geos → score as Borderline signal (tech fit may be strong, delivery uncertain)
_GEO_STRONG = [r"\bindia\b", r"\bafghanistan\b", r"\btajikistan\b",
               r"\bbangladesh\b", r"\bsri lanka\b", r"\bnepal\b"]
_GEO_WEAK   = [r"\bchina\b", r"\btanzania\b", r"\bkenya\b", r"\bkenya\b",
               r"\bnigeria\b", r"\bethiopia\b", r"\bghana\b", r"\blatin america\b",
               r"\bsub.saharan\b", r"\bsouth africa\b", r"\bcolombia\b",
               r"\bperu\b", r"\bbrazil\b"]

# Portals known to produce high-quality consulting tenders
_INTL_PORTALS = {
    "World Bank", "worldbank", "UNDP Procurement", "UNGM", "AfDB Consultants",
    "GIZ India", "NGO Box", "Welthungerhilfe", "DTVP Germany",
    "GeM BidPlus", "TED EU", "AFD France", "IUCN Procurement",
    "ILO Procurement", "DevNet India",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _hits(text: str, patterns: List[str]) -> int:
    return sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))


# ── Feature engineering ────────────────────────────────────────────────────────

FEATURE_NAMES = [
    # Service signals (7)
    "svc_evaluation", "svc_mne", "svc_baseline_endline", "svc_tpm_iva",
    "svc_ta_advisory", "svc_capacity_building", "svc_research_study",
    # Hard-gate signals (2)
    "ic_role_detected", "goods_works_detected",
    # Sector signals (6)
    "sec_education", "sec_agriculture", "sec_environment_forestry",
    "sec_health_nutrition", "sec_energy", "sec_governance",
    # Client signals (3)
    "client_tier1_donor", "client_tier2_partner", "client_national_gov",
    # Geography signals — technical fit vs delivery feasibility (2)
    "geo_strong_presence", "geo_weak_presence",
    # Signal maturity signals (3)
    "maturity_signal_first", "maturity_package_first", "has_deep_text",
    # Portal quality (1)
    "portal_international",
    # Existing scores (2)
    "existing_priority_score_norm", "existing_relevance_score_norm",
    # Composite signal count (1)
    "multi_service_signal_count",
]  # Total: 27 features


def build_features(row: Dict[str, Any]) -> np.ndarray:
    """
    Build a 27-element float32 feature vector from a tender row dict.

    Accepts both workbook-style keys (Title, Organization, etc.) and
    pipeline-style keys (title, org, organization, etc.).
    """
    title   = str(row.get("title")   or row.get("Title")        or "")
    sector  = str(row.get("sector")  or row.get("Sector")        or "")
    stype   = str(row.get("service_type") or row.get("Service Type") or "")
    org     = str(row.get("org") or row.get("organization") or row.get("Organization") or "")
    country = str(row.get("country") or row.get("Country")       or "")
    portal  = str(row.get("portal")  or row.get("Portal")        or "")
    ps      = float(row.get("priority_score") or row.get("Priority Score") or 0)
    rs      = float(row.get("relevance_score") or row.get("Relevance Score") or 0)
    deep    = str(row.get("deep_scope") or row.get("Deep Scope") or row.get("rich_text") or "")
    ai_sum  = str(row.get("ai_summary") or row.get("AI Summary") or "")

    combined = (title + " " + sector + " " + stype)

    # ── Service signals ────────────────────────────────────────────────────────
    f_eval    = float(bool(_hits(combined, _SVC_EVAL)))
    f_mne     = float(bool(_hits(combined, _SVC_MNE)))
    f_base    = float(bool(_hits(combined, _SVC_BASE)))
    f_tpm     = float(bool(_hits(combined, _SVC_TPM)))
    f_ta      = float(bool(_hits(combined, _SVC_TA)))
    f_cap     = float(bool(_hits(combined, _SVC_CAP)))
    f_research = float(bool(_hits(combined, _SVC_RSRCH)))

    # ── Hard-gate signals ──────────────────────────────────────────────────────
    f_ic    = float(bool(_hits(title, _IC_ROLE)))
    f_goods = float(bool(_hits(combined, _GOODS_WRK)))

    # ── Sector signals ─────────────────────────────────────────────────────────
    f_edu    = float(bool(_hits(combined, _SEC_EDU)))
    f_agr    = float(bool(_hits(combined, _SEC_AGR)))
    f_env    = float(bool(_hits(combined, _SEC_ENV)))
    f_health = float(bool(_hits(combined, _SEC_HEALTH)))
    f_energy = float(bool(_hits(combined, _SEC_ENERGY)))
    f_gov    = float(bool(_hits(combined, _SEC_GOV)))

    # ── Client signals ─────────────────────────────────────────────────────────
    f_cli_t1  = 1.0 if org.strip() in _CLIENT_T1 else 0.0
    f_cli_t2  = float(bool(_hits(org, _CLIENT_T2_PAT)))
    f_natl_gov = float(bool(_hits(org, _NATL_GOV)))

    # ── Geography signals (technical fit vs delivery feasibility) ──────────────
    geo_combined = country + " " + title
    f_geo_strong = float(bool(_hits(geo_combined, _GEO_STRONG)))
    f_geo_weak   = float(bool(_hits(geo_combined, _GEO_WEAK)))

    # ── Signal maturity proxy ──────────────────────────────────────────────────
    title_words    = len(title.split())
    f_signal_first = 1.0 if title_words < 8  else 0.0   # short title = early signal
    f_pkg_first    = 1.0 if title_words >= 15 else 0.0   # detailed title = full package
    f_has_deep     = 1.0 if (len(deep) > 50 or len(ai_sum) > 50) else 0.0

    # ── Portal quality ─────────────────────────────────────────────────────────
    f_portal_intl = 1.0 if portal in _INTL_PORTALS else 0.0

    # ── Existing scores (normalized 0–1) ───────────────────────────────────────
    f_ps_norm = min(ps, 100.0) / 100.0
    f_rs_norm = min(rs, 100.0) / 100.0

    # ── Composite: total consulting service signals (normalized) ───────────────
    n_svc = f_eval + f_mne + f_base + f_tpm + f_ta + f_cap + f_research
    f_multi = min(1.0, n_svc / 3.0)

    return np.array([
        f_eval, f_mne, f_base, f_tpm, f_ta, f_cap, f_research,  # 7
        f_ic, f_goods,                                            # 2
        f_edu, f_agr, f_env, f_health, f_energy, f_gov,          # 6
        f_cli_t1, f_cli_t2, f_natl_gov,                          # 3
        f_geo_strong, f_geo_weak,                                 # 2
        f_signal_first, f_pkg_first, f_has_deep,                  # 3
        f_portal_intl,                                             # 1
        f_ps_norm, f_rs_norm,                                     # 2
        f_multi,                                                   # 1
    ], dtype=np.float32)  # 27 features


# ── Model class ────────────────────────────────────────────────────────────────

class IDCGLabelModel:
    """Gradient Boosting shadow label model. Predicts Relevant/Borderline/Irrelevant."""

    def __init__(self) -> None:
        self._clf = None
        self._meta: Dict[str, Any] = {}
        self._loaded = False

    # ── Persistence ────────────────────────────────────────────────────────────

    def save(self) -> None:
        _MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        if _joblib:
            _joblib.dump(self._clf, _MODEL_PATH)
        else:
            with _MODEL_PATH.open("wb") as fh:
                pickle.dump(self._clf, fh)
        with _META_PATH.open("w") as fh:
            json.dump(self._meta, fh, indent=2)
        _log.info("[label_model] saved to %s", _MODEL_PATH)

    def load(self) -> bool:
        if not _MODEL_PATH.exists():
            _log.debug("[label_model] no artifact found at %s", _MODEL_PATH)
            return False
        try:
            if _joblib:
                self._clf = _joblib.load(_MODEL_PATH)
            else:
                with _MODEL_PATH.open("rb") as fh:
                    self._clf = pickle.load(fh)
            if _META_PATH.exists():
                with _META_PATH.open() as fh:
                    self._meta = json.load(fh)
            self._loaded = True
            _log.info("[label_model] loaded from %s (trained %s)",
                      _MODEL_PATH, self._meta.get("trained_at", "?"))
            return True
        except Exception as exc:
            _log.warning("[label_model] load failed: %s", exc)
            return False

    # ── Training ───────────────────────────────────────────────────────────────

    def train(
        self,
        rows: List[Dict[str, Any]],
        trained_at: str = "",
    ) -> Dict[str, Any]:
        """
        Train on labeled rows.  Each row must have a 'label' key.
        Returns a metrics dict.
        """
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import cross_val_predict, StratifiedKFold
        from sklearn.metrics import classification_report, confusion_matrix

        X = np.array([build_features(r) for r in rows], dtype=np.float32)
        y = np.array([LABEL_MAP.get(str(r.get("label", "")), 1) for r in rows])

        # Higher weight for human-reviewed rows vs AI-only
        sample_weight = np.array([
            1.0 if r.get("label_source", "human") == "human" else 0.7
            for r in rows
        ])

        # Shallow GBM: good bias/variance trade-off for ~350 rows
        clf = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.10,
            subsample=0.80,
            min_samples_leaf=3,
            random_state=42,
        )

        # 5-fold stratified CV for honest validation metrics
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        y_cv = cross_val_predict(clf, X, y, cv=cv)

        report = classification_report(
            y, y_cv,
            labels=[0, 1, 2],
            target_names=["Irrelevant", "Borderline", "Relevant"],
            output_dict=True,
            zero_division=0,
        )
        cm = confusion_matrix(y, y_cv, labels=[0, 1, 2]).tolist()

        # Fit final model on all data with sample weights
        clf.fit(X, y, sample_weight=sample_weight)
        self._clf = clf
        self._loaded = True

        # Feature importance — top 10
        fi_pairs = sorted(
            zip(FEATURE_NAMES, clf.feature_importances_),
            key=lambda x: -x[1],
        )
        fi_top10 = [(name, round(float(imp), 4)) for name, imp in fi_pairs[:10]]

        label_dist = {
            "Relevant":   int(np.sum(y == 2)),
            "Borderline": int(np.sum(y == 1)),
            "Irrelevant": int(np.sum(y == 0)),
        }

        # Compact CV summary
        cv_summary = {
            cls: {
                "precision": round(report[cls]["precision"], 3),
                "recall":    round(report[cls]["recall"], 3),
                "f1":        round(report[cls]["f1-score"], 3),
                "support":   int(report[cls]["support"]),
            }
            for cls in ["Irrelevant", "Borderline", "Relevant"]
        }
        cv_summary["accuracy"] = round(report["accuracy"], 3)
        cv_summary["macro_f1"] = round(report["macro avg"]["f1-score"], 3)

        self._meta = {
            "trained_at":         trained_at,
            "n_samples":          len(rows),
            "label_distribution": label_dist,
            "cv_summary":         cv_summary,
            "confusion_matrix":   cm,
            "feature_importance_top10": fi_top10,
            "model_params": {
                "n_estimators": 100,
                "max_depth":    3,
                "learning_rate": 0.10,
                "subsample":    0.80,
            },
            "n_features": len(FEATURE_NAMES),
            "blend_weight": 0.35,
        }

        return self._meta

    # ── Inference ──────────────────────────────────────────────────────────────

    def predict_proba(self, row: Dict[str, Any]) -> np.ndarray:
        """
        Returns [P(Irrelevant), P(Borderline), P(Relevant)] or uniform [1/3,1/3,1/3].
        """
        if not self._loaded or self._clf is None:
            return np.array([1/3, 1/3, 1/3], dtype=np.float32)
        try:
            x = build_features(row).reshape(1, -1)
            return self._clf.predict_proba(x)[0]
        except Exception as exc:
            _log.debug("[label_model] predict_proba error: %s", exc)
            return np.array([1/3, 1/3, 1/3], dtype=np.float32)

    def predict_score(self, row: Dict[str, Any]) -> float:
        """
        Convert probability distribution to a 0–100 score:
          score = P(Relevant)*100 + P(Borderline)*50 + P(Irrelevant)*0
        Returns 50.0 (neutral) if model not loaded.
        """
        proba = self.predict_proba(row)
        # proba order is [Irrelevant=0, Borderline=1, Relevant=2]
        score = float(proba[2] * 100.0 + proba[1] * 50.0 + proba[0] * 0.0)
        return round(score, 2)

    def predict_label(self, row: Dict[str, Any]) -> str:
        """Return the predicted label string."""
        proba = self.predict_proba(row)
        idx = int(np.argmax(proba))
        return LABEL_INV[idx]

    def is_ready(self) -> bool:
        return self._loaded and self._clf is not None


# ── Singleton ──────────────────────────────────────────────────────────────────

_model_instance: Optional[IDCGLabelModel] = None


def get_model() -> IDCGLabelModel:
    """Return the singleton IDCGLabelModel, loading from disk on first call."""
    global _model_instance
    if _model_instance is None:
        _model_instance = IDCGLabelModel()
        _model_instance.load()
    return _model_instance


def predict_shadow_score(row: Dict[str, Any]) -> float:
    """
    Module-level convenience function.
    Returns a 0–100 shadow ML score.
    Returns 50.0 (neutral Borderline) if model artifact not yet available.
    """
    try:
        return get_model().predict_score(row)
    except Exception as exc:
        _log.debug("[label_model] predict_shadow_score fallback: %s", exc)
        return 50.0


def get_shadow_note(row: Dict[str, Any]) -> str:
    """
    Return a short scoring note fragment based on shadow model prediction
    and the technical-fit / delivery-feasibility heuristic.

    Note fragments:
      - "ML strong IDCG fit"
      - "ML borderline IDCG fit"
      - "Technically strong, delivery uncertain"
      - "Signal-first notice"
      - "Package-first, strong evidence"
    """
    try:
        m = get_model()
        if not m.is_ready():
            return ""
        proba = m.predict_proba(row)
        p_rel, p_brd, p_irr = float(proba[2]), float(proba[1]), float(proba[0])

        title = str(row.get("title") or row.get("Title") or "")
        country = str(row.get("country") or row.get("Country") or "")
        deep = str(row.get("deep_scope") or row.get("rich_text") or "")

        title_words   = len(title.split())
        has_weak_geo  = bool(_hits(country + " " + title, _GEO_WEAK))
        has_strong_svc = bool(
            _hits(title, _SVC_EVAL) or _hits(title, _SVC_MNE) or
            _hits(title, _SVC_BASE) or _hits(title, _SVC_TPM)
        )
        is_signal_first = title_words < 8
        is_pkg_first    = title_words >= 15
        has_deep        = len(deep) > 50

        parts = []

        # Technical fit vs delivery feasibility
        if has_weak_geo and has_strong_svc and p_rel > 0.30:
            parts.append("Technically strong, delivery uncertain")
        elif p_rel >= 0.55:
            parts.append("ML strong IDCG fit")
        elif p_brd >= 0.50:
            parts.append("ML borderline IDCG fit")

        # Signal maturity
        if is_signal_first and not has_deep:
            parts.append("Signal-first notice")
        elif is_pkg_first and has_strong_svc:
            parts.append("Package-first, strong evidence")

        return "; ".join(parts)
    except Exception as exc:
        _log.debug("[label_model] get_shadow_note error: %s", exc)
        return ""
