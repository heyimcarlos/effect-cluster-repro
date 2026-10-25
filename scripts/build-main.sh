#!/usr/bin/env bash
# Builds Effect main (or REF) and packs effect, @effect/sql-pg,
# @effect/platform-node-shared and @effect/platform-node into .build/main/,
# where `./run-all.sh main` installs them from. Needs git, node and pnpm.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
work=${WORK_DIR:-$(mktemp -d)}
out=$repo/.build/main
ref=${REF:-837e906d9a476bff12fbd063d1635f748e1de7de}

git clone --quiet --filter=blob:none --no-checkout https://github.com/Effect-TS/effect.git "$work/effect"
cd "$work/effect"
git checkout --quiet --detach "$ref"
pnpm install --frozen-lockfile
rm -rf "$out" && mkdir -p "$out"
for p in packages/effect packages/sql/pg packages/platform/node-shared packages/platform/node; do
  (cd "$p" && pnpm build && pnpm pack --pack-destination "$out")
done
echo "built $(ls "$out") from $work/effect at $(git rev-parse HEAD)"
