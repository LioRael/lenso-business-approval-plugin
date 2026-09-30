#!/usr/bin/env python3
"""Read manifests and reject a change to this repository's private package policy."""

import argparse
from pathlib import Path
import sys
import tomllib


def check(root: Path) -> list[str]:
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    workspace = manifest["workspace"]
    if workspace.get("package", {}).get("publish") is not False:
        raise ValueError("workspace.package.publish must remain false")

    packages = []
    for member in workspace["members"]:
        directory = (root / member).resolve()
        directory.relative_to(root)
        package = tomllib.loads((directory / "Cargo.toml").read_text())["package"]
        publish = package.get("publish")
        inherited = (
            isinstance(publish, dict)
            and set(publish) == {"workspace"}
            and publish["workspace"] is True
        )
        if publish is not False and not inherited:
            raise ValueError(f"{package['name']}: publish must be false or inherit the private workspace")
        packages.append(package["name"])
    return packages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        packages = check(args.root.resolve())
    except (KeyError, OSError, ValueError) as error:
        print(f"package policy check failed: {error}", file=sys.stderr)
        return 1
    print(f"Private package policy verified for {len(packages)} workspace packages; no publication performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
