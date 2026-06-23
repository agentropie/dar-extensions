#!/usr/bin/env bash
#
# Re-pin every dar extension to the dar checkout's current git HEAD, so the
# agent's composed crate and the extensions resolve to ONE host-api version.
#
# Run this after updating dar (e.g. `git -C ~/code/agentropy/dar pull` +
# `cargo install --path dist`), since `agentropy build` pins stock dar crates to
# your dar checkout's live HEAD — extensions on an older rev fail to link.
#
# Usage:
#   ./bump.sh                 # bump all extensions to dar HEAD
#   ./bump.sh <agent-dir>     # also run `agentropy lock-refresh && build` there
#
# Env overrides:
#   DAR_REPO   path to the dar checkout       (default: ~/code/agentropy/dar)
#   EXT_DIR    path to this extensions folder (default: this script's dir)

set -euo pipefail

DAR="${DAR_REPO:-$HOME/code/agentropy/dar}"
EXT="${EXT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
AGENT="${1:-}"

if [[ ! -d "$DAR/.git" ]]; then
  echo "error: dar checkout not found at $DAR (set DAR_REPO)" >&2
  exit 1
fi

REV=$(git -C "$DAR" rev-parse HEAD)
echo "dar HEAD: $REV"

shopt -s nullglob
bumped=0
for f in "$EXT"/*/Cargo.toml; do
  grep -q 'dar\.git' "$f" || continue
  # Replace the 40-hex rev only on lines pinning the dar git repo.
  sed -i '' -E "s|(dar\.git\"[, ]*[, ]*rev = \")[0-9a-f]{40}|\1$REV|g" "$f"
  echo "  bumped $(basename "$(dirname "$f")")"
  bumped=$((bumped + 1))
done
echo "re-pinned $bumped extension(s) to $REV"

if [[ -z "$AGENT" ]]; then
  echo "next: cd <agent> && agentropy lock-refresh && agentropy build"
  exit 0
fi

echo "refreshing + building agent at $AGENT"
( cd "$AGENT" && agentropy lock-refresh && agentropy build )
echo "done."
