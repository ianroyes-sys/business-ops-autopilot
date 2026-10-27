#!/usr/bin/env python3
"""Per-loop mkdir lease. A kernel lock serializes stale-owner recovery and release."""
import argparse
import fcntl
import os
from pathlib import Path
import time


def alive(pid):
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def lease(directory, pid, action):
    directory = Path(directory)
    directory.parent.mkdir(parents=True, exist_ok=True)
    # Never unlink the guard inode: every contender must lock the same inode.
    with directory.with_suffix(".guard").open("a") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        owner = None
        if directory.exists():
            try:
                owner = int((directory / "pid").read_text().strip())
            except (OSError, ValueError):
                pass
        if action == "release":
            if owner != pid:
                return False
            (directory / "pid").unlink()
            directory.rmdir()
            return True
        if directory.exists():
            if owner is not None and alive(owner):
                return False
            # If interrupted between mkdir and owner write, allow time for creation to
            # settle, then recover. The flock prevents two recoverers deleting a new owner.
            if owner is None and time.time() - directory.stat().st_mtime < 30:
                return False
            (directory / "pid").unlink(missing_ok=True)
            directory.rmdir()
        directory.mkdir()
        (directory / "pid").write_text(str(pid) + "\n")
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["acquire", "release"])
    parser.add_argument("directory", type=Path)
    parser.add_argument("pid", type=int)
    args = parser.parse_args()
    if args.pid <= 0:
        parser.error("pid must be positive")
    raise SystemExit(0 if lease(args.directory, args.pid, args.action) else 75)
