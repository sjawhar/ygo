#!/usr/bin/env python3
"""Replay the preserved test suite against an isolated runtime worktree."""

import argparse
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile


HERE = Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / "manifest.json").read_text())


def git(repo, *args, binary=False):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=not binary)


def functions(files):
    result = set()
    for path, content in files.items():
        if path.endswith("_test.go"):
            for name in re.findall(r"^func ((?:Test|Example|Fuzz)\w+)\(", content.decode(), re.M):
                result.add(path.rsplit("/", 1)[0] + "/" + name)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", required=True, type=Path, help="new, nonexistent directory for the isolated worktree")
    parser.add_argument("--legacy-repo", type=Path, help="repository containing the frozen test-source commit")
    parser.add_argument("--runtime-ref", default="HEAD")
    parser.add_argument("--node-modules", type=Path, help="copy an existing compatible testutil/node_modules instead of running npm ci")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    repo = Path(git(HERE, "rev-parse", "--show-toplevel").strip())
    legacy = (args.legacy_repo or repo).resolve()
    work = args.work_dir.resolve()
    if work.exists():
        parser.error("--work-dir must not exist; existing files will not be overwritten")
    ref = MANIFEST["legacy_ref"]
    git(legacy, "cat-file", "-e", ref + "^{commit}")
    runtime = git(repo, "rev-parse", args.runtime_ref + "^{commit}").strip()
    production = {name: git(repo, "show", runtime + ":" + name, binary=True)
                  for name in git(repo, "ls-tree", "-r", "--name-only", runtime).splitlines()
                  if name.endswith(".go") and not name.endswith("_test.go")}
    paths = [name for name in git(legacy, "ls-tree", "-r", "--name-only", ref).splitlines()
             if name.endswith("_test.go") or name.startswith("testutil/") or "/testdata/" in name]
    archive = git(legacy, "archive", ref, "--", *paths, binary=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        files = {}
        for member in tar.getmembers():
            if member.isfile():
                destination = (work / member.name).resolve()
                destination.relative_to(work)
                files[member.name] = tar.extractfile(member).read()
    original = functions(files)
    assert len(original) == MANIFEST["baseline_functions"]
    subprocess.run(["git", "-C", str(repo), "worktree", "add", "--detach", str(work), runtime], check=True)
    for file in work.rglob("*_test.go"):
        file.unlink()
    for name, content in files.items():
        destination = work / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    subprocess.run(["git", "-C", str(work), "apply", "--check", "--unidiff-zero", str(HERE / "adaptation.patch")], check=True)
    subprocess.run(["git", "-C", str(work), "apply", "--unidiff-zero", str(HERE / "adaptation.patch")], check=True)
    actual = functions({file.relative_to(work).as_posix(): file.read_bytes()
                        for file in work.rglob("*_test.go")})
    missing = sorted(MANIFEST["renames"].get(name, name) for name in original
                     if MANIFEST["renames"].get(name, name) not in actual)
    assert not missing, missing
    assert all((work / name).read_bytes() == content for name, content in production.items()), "runtime Go source changed"
    inventory = {"runtime_ref": runtime, "legacy_ref": ref, "baseline_functions": len(original),
                 "adapted_functions": len(actual), "renames": MANIFEST["renames"], "missing": missing,
                 "runtime_Go_source_unchanged": True}
    (work / "compat-inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
    print(json.dumps(inventory, indent=2), flush=True)
    if args.prepare_only:
        return 0
    if args.node_modules:
        shutil.copytree(args.node_modules.resolve(), work / "testutil/node_modules")
    else:
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=work / "testutil", check=True)
    env = os.environ.copy()
    env["YGO_REQUIRE_NODE"] = "1"
    results = {}
    for module, cwd in (("root", work), ("mobile", work / "mobile")):
        log = work / ("compat-" + module + "-tests.jsonl")
        with log.open("w") as output:
            result = subprocess.run(["go", "test", "-race", "-count=1", "-json", "-timeout", "300s", "./..."],
                                    cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
        results[module] = {"exit_code": result.returncode, "log": str(log)}
        print(module + ": exit " + str(result.returncode) + "; " + str(log), flush=True)
    (work / "compat-results.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(any(result["exit_code"] != 0 for result in results.values()))


if __name__ == "__main__":
    raise SystemExit(main())
