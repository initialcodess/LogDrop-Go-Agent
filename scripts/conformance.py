"""Keeps conformance/privacy identical to the Java agent's copy (the reference).

    python scripts/conformance.py check   - the files match conformance/SOURCE.json; when a Java agent checkout is
                                            found (LOGDROP_JAVA_AGENT_DIR or ../LogDrop-Java-Agent), they also match it
    python scripts/conformance.py update  - copies the files from that checkout and records their hashes
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCAL = ROOT / "conformance" / "privacy"
MANIFEST = ROOT / "conformance" / "SOURCE.json"
JAVA_DIR = Path(os.environ.get("LOGDROP_JAVA_AGENT_DIR", ROOT.parent / "LogDrop-Java-Agent")).resolve()
JAVA_CASES = JAVA_DIR / "conformance" / "privacy"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def json_files(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if p.suffix == ".json")


def fail(message: str) -> None:
    print(f"conformance: {message}", file=sys.stderr)
    sys.exit(1)


def update() -> None:
    if not JAVA_CASES.is_dir():
        fail(f"no Java agent checkout at {JAVA_DIR} (set LOGDROP_JAVA_AGENT_DIR)")
    files = {}
    for name in json_files(JAVA_CASES):
        shutil.copyfile(JAVA_CASES / name, LOCAL / name)
        files[name] = sha256(LOCAL / name)
    try:
        commit = subprocess.run(["git", "-C", str(JAVA_DIR), "rev-parse", "--short", "HEAD"], check=True,
                                capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        commit = "unknown"
    manifest = {"source": "LogDrop-Java-Agent/conformance/privacy", "commit": commit, "files": files}
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"conformance: copied {len(files)} files from LogDrop-Java-Agent {commit}")


def check() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    names = json_files(LOCAL)
    if names != sorted(manifest["files"]):
        fail("conformance/privacy and conformance/SOURCE.json list different files; run scripts/conformance.py update")
    for name in names:
        if sha256(LOCAL / name) != manifest["files"][name]:
            fail(f"{name} was changed here; change the Java agent's copy first, then run scripts/conformance.py update")
    if JAVA_CASES.is_dir():
        for name in json_files(JAVA_CASES):
            if manifest["files"].get(name) != sha256(JAVA_CASES / name):
                fail(f"{name} differs from the Java agent checkout at {JAVA_DIR}; run scripts/conformance.py update")
    print(f"conformance: {len(names)} files match LogDrop-Java-Agent {manifest['commit']}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "update":
        update()
    elif command == "check":
        check()
    else:
        fail("usage: conformance.py check|update")
