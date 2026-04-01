"""
Migrate outputs/week4/lineage_snapshots.jsonl (NetworkX JSON) to canonical lineage_snapshot.

Edge mapping: CONFIGURES -> PRODUCES (dataflow from config to configured artifact).

Output: outputs/migrate/week4/lineage_snapshots.jsonl
Run: python outputs/migrate/migrate_week4.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_MIGRATE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MIGRATE_DIR))

from _common import env_repo_root, iso_now, repo_root, uuid5_for, write_jsonl

_PATH_RE = re.compile(r"[./\\]")


def _infer_node_type(node_id: str) -> str:
    s = str(node_id)
    if _PATH_RE.search(s) and (s.endswith(".py") or "/" in s or "\\" in s):
        return "FILE"
    if s in ("operator",) or s.startswith("task_"):
        return "TABLE"
    return "EXTERNAL"


def _canonical_node_id(raw_id: str) -> tuple[str, str, str]:
    """Return (node_id, type, label)."""
    ntype = _infer_node_type(raw_id)
    label = Path(str(raw_id)).name if ntype == "FILE" else str(raw_id)
    nid = f"{ntype}::{str(raw_id).replace(chr(92), '/')}"
    return nid, ntype, label


def networkx_to_snapshot(graph: dict) -> dict:
    raw_nodes = graph.get("nodes") or []
    node_map: dict[str, str] = {}
    nodes_out: list[dict] = []

    for n in raw_nodes:
        if not isinstance(n, dict):
            continue
        rid = n.get("id")
        if rid is None:
            continue
        rid = str(rid)
        nid, ntype, label = _canonical_node_id(rid)
        node_map[rid] = nid
        path = str(rid).replace("\\", "/")
        meta = {
            "path": path if ntype == "FILE" else "",
            "language": "python" if path.endswith(".py") else "unknown",
            "purpose": "migrated-from-networkx-export",
            "last_modified": iso_now(),
        }
        if n.get("name"):
            meta["table_name"] = n.get("name")
        nodes_out.append(
            {
                "node_id": nid,
                "type": ntype,
                "label": label,
                "metadata": meta,
            }
        )

    snapshot_id = uuid5_for("snapshot", *sorted(node_map.values()))

    edges_out: list[dict] = []
    for e in graph.get("edges") or []:
        if not isinstance(e, dict):
            continue
        s = str(e.get("source") or "")
        t = str(e.get("target") or "")
        sid = node_map.get(s)
        tid = node_map.get(t)
        if not sid or not tid:
            continue
        edges_out.append(
            {
                "source": sid,
                "target": tid,
                "relationship": "PRODUCES",
                "confidence": 1.0,
            }
        )

    return {
        "snapshot_id": snapshot_id,
        "codebase_root": env_repo_root(),
        "git_commit": "0" * 40,
        "nodes": nodes_out,
        "edges": edges_out,
        "captured_at": iso_now(),
    }


def migrate(src: Path, dst: Path) -> int:
    data = json.loads(src.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        print("error: expected JSON object", file=sys.stderr)
        return 1
    snap = networkx_to_snapshot(data)
    write_jsonl(dst, [snap])
    print(f"wrote 1 lineage_snapshot -> {dst}")
    return 0


def main() -> int:
    root = repo_root()
    ap = argparse.ArgumentParser(description="Migrate Week 4 NetworkX graph to canonical lineage_snapshot JSONL.")
    ap.add_argument(
        "--src",
        type=Path,
        default=root / "outputs" / "week4" / "lineage_snapshots.jsonl",
    )
    ap.add_argument(
        "--dst",
        type=Path,
        default=root / "outputs" / "migrate" / "week4" / "lineage_snapshots.jsonl",
    )
    args = ap.parse_args()
    if not args.src.is_file():
        print(f"error: missing source {args.src}", file=sys.stderr)
        return 1
    return migrate(args.src, args.dst)


if __name__ == "__main__":
    raise SystemExit(main())
