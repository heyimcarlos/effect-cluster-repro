#!/usr/bin/env bash
# Runs every issue script against one build of Effect and prints a summary.
#   ./run-all.sh          stock release from npm (./node_modules)
#   ./run-all.sh main     Effect main, from .build/main (scripts/build-main.sh)
# Full output of each script goes to logs/<build>/. Exits nonzero if any
# script reproduced its bug or errored.
set -u
cd "$(dirname "$0")"
variant=${1:-stock}

if [ "$variant" = stock ]; then
  dir=.
  [ -d node_modules ] || npm install --no-audit --no-fund
else
  dir=variants/$variant
  [ -d ".build/$variant" ] || { echo "missing .build/$variant: run scripts/build-$variant.sh" >&2; exit 2; }
  rm -rf "$dir/src" "$dir/node_modules" "$dir/package-lock.json"
  cp -R src "$dir/src"
  (cd "$dir" && npm install --no-audit --no-fund --silent) || exit 2
fi
mkdir -p "logs/$variant"

summary=()
status=0
for script in "$dir"/src/issue*.ts; do
  name=$(basename "$script" .ts)
  echo "### $name ($variant)"
  node "$script" 2>&1 | tee "logs/$variant/$name.log" | grep -E '^(===|expected|actual|          |PASS|FAIL)'
  code=${PIPESTATUS[0]}
  case $code in 0) r=PASS ;; 1) r="FAIL (reproduced)" ;; *) r="ERROR ($code)" ;; esac
  [ "$code" = 0 ] || status=1
  summary+=("$name: $r")
done

echo
echo "Summary ($variant):"
printf '  %s\n' "${summary[@]}"
exit $status
