# contracts/baseline_store.py — shared numeric baselines for generator + runner
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def write_baselines(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    baselines: dict[str, dict[str, float]] = {}
    for col in df.select_dtypes(include="number").columns:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        std = float(s.std()) if len(s) > 1 else 0.0
        if std != std:
            std = 0.0
        baselines[col] = {"mean": float(s.mean()), "stddev": std}
    payload = {
        "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "columns": baselines,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def load_column_baselines(path: Path) -> dict[str, dict[str, float]]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return dict(raw.get("columns") or {})
    except (json.JSONDecodeError, OSError):
        return {}
