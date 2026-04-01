# Extract top-level keys from JSON / JSONL under outputs/ → outputs/extracted_keys.json
from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _keys_from_obj(obj: object) -> set[str]:
    if isinstance(obj, dict):
        return set(obj)
    return set()


def _keys_from_parsed(data: object) -> list[str]:
    if isinstance(data, dict):
        return sorted(data)
    if isinstance(data, list):
        keys: set[str] = set()
        for item in data:
            keys |= _keys_from_obj(item)
        return sorted(keys)
    return []


def extract_keys(path: Path) -> tuple[list[str] | None, str | None]:
    """Return (sorted keys, error message) for a single file."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return None, str(e)

    stripped = text.strip()
    if not stripped:
        return [], None

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None

    if data is not None:
        return _keys_from_parsed(data), None

    keys: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        keys |= _keys_from_obj(obj)

    if keys:
        return sorted(keys), None
    return None, "no valid JSON or JSONL records found"


def main() -> None:
    root = _repo_root()
    outputs = root / "outputs"

    if not outputs.is_dir():
        print(f"error: outputs directory not found: {outputs}", file=sys.stderr)
        sys.exit(1)

    results: list[dict[str, object]] = []
    for path in sorted(outputs.rglob("*")):
        if not path.is_file():
            continue
        if path.name == "extracted_keys.json":
            continue
        rel = path.relative_to(root).as_posix()
        keys, err = extract_keys(path)
        entry: dict[str, object] = {"path": rel}
        if err is not None:
            entry["error"] = err
        if keys is not None:
            entry["keys"] = keys
        results.append(entry)

    out_path = outputs / "extracted_keys.json"
    out_path.write_text(
        json.dumps({"files": results}, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Wrote {len(results)} file(s) to {out_path.relative_to(root)}")


if __name__ == "__main__":
    main()
