#!/usr/bin/env python3
"""Post-init hook for the lume scaffold.

Invoked by scripts/new-project.py after the scaffold has been copied to the
student's target directory. CWD is the target. Stdlib only — students have not
run `uv sync` yet.

Responsibilities:
  * Ensure data/ exists with a .gitkeep. data/ is a host-side scratch
    directory students can use freely; the vendor's downloaded parquet lives
    inside a docker named volume (meter-data), not here.
  * Copy .env.example to .env so `make run` works immediately. Never overwrite
    an existing .env (students may have customised it during a re-scaffold).
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path


def main(target: Path) -> int:
    data = target / "data"
    data.mkdir(exist_ok=True)
    (data / ".gitkeep").touch(exist_ok=True)
    print(f"[post-init] ensured {data}/.gitkeep")

    env_example = target / ".env.example"
    env = target / ".env"
    if env_example.exists() and not env.exists():
        shutil.copyfile(env_example, env)
        print(f"[post-init] copied .env.example -> .env")
    elif env.exists():
        print(f"[post-init] .env already exists; leaving as-is")
    return 0


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    sys.exit(main(target))
