# contracts/ai_extensions.py — AI Contract Extensions entry point
"""LLM-assisted column descriptions and cross-field rules (optional Claude step)."""

from __future__ import annotations

import argparse


def main() -> int:
    p = argparse.ArgumentParser(description="AI Contract Extensions: LLM annotations for ambiguous columns.")
    p.add_argument("--contract", type=str, help="Path to generated contract YAML")
    args = p.parse_args()
    print(
        "AI Contract Extensions: invoke when column semantics need human-readable rules.\n"
        f"  contract={args.contract!r}\n"
        "  Set ANTHROPIC_API_KEY and implement append llm_annotations per course spec."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
