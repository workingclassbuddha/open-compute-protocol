#!/usr/bin/env python3
"""Export the code-owned OCP HTTP contract snapshot as JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TextIO

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from server_contract import build_contract_snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export the OCP HTTP contract snapshot.")
    parser.add_argument("--output", default="", help="Optional path to write JSON output.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON with stable key order.")
    return parser


def write_snapshot(stream: TextIO, *, pretty: bool) -> None:
    snapshot = build_contract_snapshot()
    if pretty:
        json.dump(snapshot, stream, indent=2, sort_keys=True)
    else:
        json.dump(snapshot, stream, separators=(",", ":"), sort_keys=True)
    stream.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", encoding="utf-8") as stream:
                write_snapshot(stream, pretty=args.pretty)
        else:
            write_snapshot(sys.stdout, pretty=args.pretty)
    except Exception as exc:
        print(f"ERROR: failed to export contract: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
