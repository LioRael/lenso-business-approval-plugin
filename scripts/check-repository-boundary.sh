#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-business-approval-postgres-plugin\nlenso-capability-business-approval'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi

if rg -n 'path\s*=\s*"(\.\./\.\./|/)' --glob 'Cargo.toml' .; then
  echo "cross-repository or absolute path dependencies are not allowed" >&2
  exit 1
fi

if rg -n 'lenso-platform-|lenso-module-|HostBuilder|HostLinkedModule|ModuleManifest' \
  Cargo.toml crates README.md CONTEXT.md --glob '!**/generated.rs'; then
  echo "legacy Lenso framework dependency or API found" >&2
  exit 1
fi
