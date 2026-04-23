from __future__ import annotations

import argparse
import plistlib
import shutil
import stat
from pathlib import Path

DEFAULT_APP_NAME = "OCP"
DEFAULT_BUNDLE_ID = "org.opencomputeprotocol.ocp"

EXCLUDED_DIR_NAMES = {
    ".git",
    ".local",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    "env",
    "htmlcov",
    "identities",
    "identity",
    "logs",
    "node_modules",
    "tests",
    "tmp",
    "venv",
    ".venv",
    ".vscode",
    ".idea",
}
EXCLUDED_FILE_NAMES = {
    ".coverage",
    ".DS_Store",
    ".env",
    "launcher.json",
    "ocp.db",
}
EXCLUDED_FILE_PREFIXES = (
    ".env.",
)
EXCLUDED_FILE_SUFFIXES = {
    ".db",
    ".db-shm",
    ".db-wal",
    ".der",
    ".crt",
    ".err",
    ".key",
    ".log",
    ".out",
    ".p12",
    ".pem",
    ".pid",
    ".sqlite",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
    ".tmp",
    ".pyc",
    ".pyo",
}


def should_exclude(path: Path, repo_root: Path) -> bool:
    try:
        relative = Path(path).resolve().relative_to(Path(repo_root).resolve())
    except ValueError:
        relative = Path(path)
    parts = relative.parts
    if any(part in EXCLUDED_DIR_NAMES or part.startswith(".mesh") for part in parts):
        return True
    name = Path(path).name
    if name in EXCLUDED_FILE_NAMES:
        return True
    if any(name.startswith(prefix) for prefix in EXCLUDED_FILE_PREFIXES):
        return True
    if name.startswith(".mesh"):
        return True
    if Path(path).suffix.lower() in EXCLUDED_FILE_SUFFIXES:
        return True
    return False


def _copy_repo(repo_root: Path, destination: Path) -> None:
    for source in Path(repo_root).iterdir():
        if should_exclude(source, repo_root):
            continue
        target = destination / source.name
        if source.is_dir():
            shutil.copytree(
                source,
                target,
                ignore=lambda directory, names: [
                    name for name in names if should_exclude(Path(directory) / name, repo_root)
                ],
            )
        else:
            shutil.copy2(source, target)


def _write_info_plist(contents_dir: Path, *, app_name: str, bundle_id: str) -> None:
    plist = {
        "CFBundleDevelopmentRegion": "en",
        "CFBundleDisplayName": app_name,
        "CFBundleExecutable": app_name,
        "CFBundleIdentifier": bundle_id,
        "CFBundleInfoDictionaryVersion": "6.0",
        "CFBundleName": app_name,
        "CFBundlePackageType": "APPL",
        "CFBundleShortVersionString": "0.1.0",
        "CFBundleVersion": "1",
        "LSMinimumSystemVersion": "12.0",
        "NSHighResolutionCapable": True,
    }
    with (contents_dir / "Info.plist").open("wb") as handle:
        plistlib.dump(plist, handle)


def _write_launcher_executable(macos_dir: Path, *, app_name: str) -> Path:
    executable = macos_dir / app_name
    executable.write_text(
        """#!/bin/sh
APP_CONTENTS="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$APP_CONTENTS/Resources/open-compute-protocol"
cd "$REPO_ROOT" || exit 1
exec /usr/bin/env python3 -m ocp_desktop.launcher --repo-root "$REPO_ROOT" "$@"
""",
        encoding="utf-8",
    )
    current = executable.stat().st_mode
    executable.chmod(current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return executable


def build_macos_app(
    repo_root: Path,
    *,
    dist_dir: Path | None = None,
    app_name: str = DEFAULT_APP_NAME,
    bundle_id: str = DEFAULT_BUNDLE_ID,
) -> dict[str, str]:
    root = Path(repo_root).resolve()
    output_dir = Path(dist_dir).resolve() if dist_dir else root / "dist"
    app_dir = output_dir / f"{app_name}.app"
    contents_dir = app_dir / "Contents"
    macos_dir = contents_dir / "MacOS"
    resources_dir = contents_dir / "Resources"
    bundled_repo = resources_dir / "open-compute-protocol"

    if app_dir.exists():
        shutil.rmtree(app_dir)
    macos_dir.mkdir(parents=True, exist_ok=True)
    bundled_repo.mkdir(parents=True, exist_ok=True)

    _write_info_plist(contents_dir, app_name=app_name, bundle_id=bundle_id)
    executable = _write_launcher_executable(macos_dir, app_name=app_name)
    _copy_repo(root, bundled_repo)

    return {
        "status": "ok",
        "app_path": str(app_dir),
        "executable": str(executable),
        "bundled_repo": str(bundled_repo),
        "note": "Unsigned beta bundle. Requires python3 to be installed on the Mac.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an unsigned macOS OCP.app beta bundle.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--dist-dir", default="")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME)
    parser.add_argument("--bundle-id", default=DEFAULT_BUNDLE_ID)
    args = parser.parse_args(argv)
    result = build_macos_app(
        Path(args.repo_root),
        dist_dir=Path(args.dist_dir) if args.dist_dir else None,
        app_name=args.app_name,
        bundle_id=args.bundle_id,
    )
    print(f"Built {result['app_path']}")
    print(result["note"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
