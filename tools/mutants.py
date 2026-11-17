"""Mutation check: each deliberately broken variant must make the test suite fail.

    python tools/mutants.py      (from the repo root)

Copies changedet/ and tests/ to a temp dir, applies one textual break, runs unittest there. Exit 0 only if every
mutant is caught and the unmutated copy passes.
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

KEYHASH_DELETE = '''        for k in [k for k in self.cache.rows if k not in remote]:
            self._settle(k, None)
'''
BUCKET_DELETE = '''            for k in [k for k in self.cache.members[b] if k not in remote]:
                self._settle(k, None)
'''
KEYHASH_FETCH = '''        changed = [k for k, h in remote.items() if self.cache.hash_of(k) != h]
        self._fetch_and_settle(self.agent, changed)


class BucketDiff'''
BUCKET_FETCH = '''        changed = [k for k, h in remote.items() if self.cache.hash_of(k) != h]
        self._fetch_and_settle(self.agent, changed)


class LocalSnapshot'''

MUTANTS = {
    "skip deletes (KeyHashSnapshot)": ("detect.py", KEYHASH_DELETE, ""),
    "skip deletes (BucketDiff)": ("detect.py", BUCKET_DELETE, ""),
    "ignore one field in the hash": ("agent.py", 'b"".join(_enc(row[f]) for f in FIELDS)',
                                     'b"".join(_enc(row[f]) for f in FIELDS[:-1])'),
    "drop the key from the hash": ("agent.py", 'return _enc(row["id"]) + b""', 'return b""'),
    "store agent-returned row hashes (KeyHashSnapshot)": (
        "detect.py", KEYHASH_FETCH,
        KEYHASH_FETCH.replace("changed)\n", "changed)\n        self.cache.hashes.update("
                              "{k: remote[k] for k in changed if k in self.cache.hashes})\n", 1)),
    "store agent-returned bucket hashes (BucketDiff)": (
        "detect.py", BUCKET_FETCH,
        BUCKET_FETCH.replace("changed)\n", "changed)\n        for i in changed_b:\n"
                             "            self.cache.buckets[i] = remote_buckets[i]\n", 1)),
    "no retry on QueueFull": ("detect.py", "except QueueFull:", "except ZeroDivisionError:"),
}


def run_suite(root):
    r = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
                       cwd=root, capture_output=True, text=True)
    return r.returncode == 0


def copy_tree(dst):
    for d in ("changedet", "tests"):
        shutil.copytree(os.path.join(HERE, d), os.path.join(dst, d),
                        ignore=shutil.ignore_patterns("__pycache__"))


def main():
    ok = True
    with tempfile.TemporaryDirectory() as tmp:
        base = os.path.join(tmp, "base")
        copy_tree(base)
        if not run_suite(base):
            print("FAIL: unmutated copy does not pass")
            return 1
        for name, (fname, old, new) in MUTANTS.items():
            root = os.path.join(tmp, str(abs(hash(name))))
            copy_tree(root)
            path = os.path.join(root, "changedet", fname)
            with open(path, encoding="utf-8") as f:
                src = f.read()
            if src.count(old) != 1:
                print(f"ERROR  {name}: anchor found {src.count(old)} times")
                ok = False
                continue
            with open(path, "w", encoding="utf-8") as f:
                f.write(src.replace(old, new))
            caught = not run_suite(root)
            print(f"{'caught ' if caught else 'MISSED '} {name}")
            ok = ok and caught
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
