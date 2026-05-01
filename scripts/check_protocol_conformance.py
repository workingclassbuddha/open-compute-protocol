#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from server_contract import build_contract_snapshot


def main() -> int:
    snapshot = build_contract_snapshot()
    conformance = dict(snapshot.get("conformance") or {})
    fixtures = list(conformance.get("fixtures") or [])
    invalid_fixtures = [fixture["id"] for fixture in fixtures if fixture.get("validation", {}).get("status") != "ok"]
    missing_request_refs = [
        endpoint["id"]
        for endpoint in snapshot.get("endpoints", [])
        if endpoint.get("request", {}).get("schema_ref")
        and endpoint["request"]["schema_ref"] not in snapshot.get("schemas", {})
    ]
    unresolved_response_refs = [
        endpoint["id"]
        for endpoint in snapshot.get("endpoints", [])
        if endpoint.get("response", {}).get("schema_ref") and not endpoint["response"].get("schema_available", False)
    ]
    generic_response_refs = [
        endpoint["id"]
        for endpoint in snapshot.get("endpoints", [])
        if endpoint.get("response", {}).get("schema_ref") == "Object"
    ]

    errors: list[str] = []
    if snapshot.get("status") != "ok":
        errors.append(f"contract snapshot status is {snapshot.get('status')!r}")
    if conformance.get("status") != "ok":
        errors.append(f"conformance snapshot status is {conformance.get('status')!r}")
    if conformance.get("invalid_fixture_count", 0) != 0:
        errors.append(f"invalid fixture count is {conformance.get('invalid_fixture_count')}")
    if invalid_fixtures:
        errors.append(f"invalid fixtures: {', '.join(invalid_fixtures)}")
    if missing_request_refs:
        errors.append(f"missing request schema refs for {len(missing_request_refs)} endpoints")

    summary = (
        f"Protocol conformance snapshot: endpoints={snapshot.get('endpoint_count', 0)} "
        f"schemas={snapshot.get('schema_count', 0)} "
        f"fixtures={conformance.get('fixture_count', 0)} "
        f"unresolved_response_refs={len(unresolved_response_refs)} "
        f"generic_response_refs={len(generic_response_refs)}"
    )
    print(summary)

    if generic_response_refs:
        preview = ", ".join(generic_response_refs[:10])
        remainder = len(generic_response_refs) - min(len(generic_response_refs), 10)
        suffix = f" (+{remainder} more)" if remainder > 0 else ""
        print(f"Response schema coverage still generic: {preview}{suffix}")

    if unresolved_response_refs:
        preview = ", ".join(unresolved_response_refs[:10])
        remainder = len(unresolved_response_refs) - min(len(unresolved_response_refs), 10)
        suffix = f" (+{remainder} more)" if remainder > 0 else ""
        print(f"Response schema coverage still partial: {preview}{suffix}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print("Protocol conformance OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
