#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT_DIR/venv_stable/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "[retrain] ERROR: expected project interpreter at $VENV_PY" >&2
  exit 1
fi

MODEL_PATH="$ROOT_DIR/artifacts/label_model.joblib"
META_PATH="$ROOT_DIR/artifacts/label_model_meta.json"

model_mtime_before=0
if [[ -f "$MODEL_PATH" ]]; then
  model_mtime_before="$(stat -f '%m' "$MODEL_PATH")"
fi

final_pipeline_ok="No"
label_summary_json="{}"

run_step() {
  local label="$1"
  shift
  echo
  echo "[retrain] $label"
  echo "[retrain] Command: $*"
  "$@"
}

run_step "Step 1/4 DB preflight" \
  "$VENV_PY" "$ROOT_DIR/scripts/test_db_connection.py"

run_step "Step 2/4 label sync run" \
  "$VENV_PY" "$ROOT_DIR/main.py"

run_step "Step 3/4 model training" \
  "$VENV_PY" "$ROOT_DIR/scripts/train_label_model.py"

run_step "Step 4/4 apply updated model" \
  "$VENV_PY" "$ROOT_DIR/main.py"
final_pipeline_ok="Yes"

label_summary_json="$("$VENV_PY" "$ROOT_DIR/scripts/workbook_label_summary.py" --json)"

echo
echo "[retrain] Final summary"
"$VENV_PY" - <<'PY' "$META_PATH" "$MODEL_PATH" "$model_mtime_before" "$final_pipeline_ok" "$label_summary_json"
import json
import os
import sys

meta_path, model_path, model_mtime_before, final_pipeline_ok, label_summary_json = sys.argv[1:6]
model_mtime_before = int(model_mtime_before or 0)

meta = {}
if os.path.exists(meta_path):
    with open(meta_path, "r", encoding="utf-8") as fh:
        meta = json.load(fh)

label_summary = json.loads(label_summary_json or "{}")
model_updated = os.path.exists(model_path) and int(os.path.getmtime(model_path)) > model_mtime_before
label_dist = meta.get("label_distribution", {})
source_counts = meta.get("label_source_counts", {})

print(f"[retrain] Approved labeled rows used : {meta.get('approved_rows_used', meta.get('n_samples', 0))}")
if label_dist:
    print(
        "[retrain] Label distribution        : "
        f"Relevant={label_dist.get('Relevant', 0)}, "
        f"Borderline={label_dist.get('Borderline', 0)}, "
        f"Irrelevant={label_dist.get('Irrelevant', 0)}"
    )
if source_counts:
    print(
        "[retrain] Label sources             : "
        f"human={source_counts.get('human', 0)}, ai={source_counts.get('ai', 0)}"
    )
print(f"[retrain] Model artifact updated     : {'Yes' if model_updated else 'No'}")
print(f"[retrain] Final pipeline run         : {final_pipeline_ok}")
if label_summary:
    print(f"[retrain] Workbook Human_Label rows  : {label_summary.get('human_label_rows', 0)}")
    print(f"[retrain] Workbook Approved=Yes rows : {label_summary.get('training_approved_yes_rows', 0)}")
    print(f"[retrain] New unlabeled rows remain  : {label_summary.get('new_unlabeled_rows', 0)}")
PY
