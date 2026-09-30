#!/usr/bin/env bash
# validate-offline-bundle.sh — prove a bundle made by make-offline-bundle.sh installs and runs
# with NO registry access, exercising every way a consumer might install it.
#
#   node/scripts/validate-offline-bundle.sh <bundle.tar.gz>
#
# Network is denied two ways: every npm invocation gets --registry pointed at a closed
# local port AND --offline. If npm needs anything it does not have, it fails loudly.
set -euo pipefail

# When invoked through `npm run`, the parent npm exports its own configuration as npm_config_*
# environment variables (cache, prefix, registry, ...). Environment beats the project .npmrc,
# which would silently redirect the cache. Start from a clean npm configuration.
for v in $(env | sed -n 's/^\(npm_config_[^=]*\)=.*/\1/p'); do unset "$v"; done
unset npm_package_json npm_lifecycle_event npm_lifecycle_script npm_execpath npm_node_execpath 2>/dev/null || true
BUNDLE="${1:?bundle.tar.gz}"
BUNDLE="$(cd "$(dirname "$BUNDLE")" && pwd)/$(basename "$BUNDLE")"
DEAD_REGISTRY="http://127.0.0.1:9"     # discard port: connection refused
OPT="$(mktemp -d)"                     # install root
trap 'rm -rf "$OPT"' EXIT
pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; exit 1; }

echo "==> 1. extract: tar -xzf <bundle> -C <install root>"
tar -xzf "$BUNDLE" -C "$OPT"
NODE_DIR="$OPT/valkey-polyglot-benchmark/node"
[ -d "$NODE_DIR" ] || fail "layout: node/ missing"
[ -f "$NODE_DIR/.npmrc" ] && { [ -d "$NODE_DIR/.npm-cache" ] || [ -d "$NODE_DIR/node_modules" ]; } || fail "bundle incomplete"
pass "layout: node/{.npmrc,.npm-cache,node_modules,package-lock.json}"
grep -q '"@valkey/valkey-glide": "\^' "$NODE_DIR/package.json" && fail "glide still a floating range" || pass "glide pinned exactly: $(node -p "require('$NODE_DIR/package.json').dependencies['@valkey/valkey-glide']")"

cd "$NODE_DIR"
export npm_config_registry="$DEAD_REGISTRY"   # belt: even if .npmrc were ignored, no registry
export npm_config_update_notifier=false

echo "==> 2. plain npm install (must be a no-op, no network)"
if out="$(npm install 2>&1)"; then pass "npm install ok offline: $(echo "$out" | tail -1)"; else echo "$out"; fail "npm install"; fi

HAS_CACHE=0; [ -d .npm-cache ] && HAS_CACHE=1
echo "==> 3. fresh reinstall from the bundled cache only: rm -rf node_modules && npm ci --offline"
if [ $HAS_CACHE = 1 ]; then
  rm -rf node_modules
  if out="$(npm ci --offline 2>&1)"; then pass "npm ci --offline: $(echo "$out" | tail -1)"; else echo "$out"; fail "npm ci --offline"; fi
else
  echo "  [SKIP] vendored-only bundle (no .npm-cache)"
fi

echo "==> 4. integrity: lock in sync, every dep present"
npm ls --omit=dev >/dev/null 2>&1 && pass "npm ls clean (lock <-> tree in sync)" || fail "npm ls reports problems"

echo "==> 5. native binding loads and the benchmark starts"
node -e "const g=require('@valkey/valkey-glide'); if(!g.GlideClient) throw new Error('no GlideClient'); console.log('glide loaded, native ok')" && pass "require('@valkey/valkey-glide') incl. native .node" || fail "glide native load"
node valkey-benchmark.js --help >/dev/null 2>&1 && pass "node valkey-benchmark.js --help" || fail "--help"

echo "==> 6. tamper check: corrupting a cached tarball must make npm ci fail"
if [ $HAS_CACHE = 1 ]; then
rm -rf node_modules
tgz="$(find .npm-cache/_cacache/content-v2 -type f -print -quit)"
cp "$tgz" "$tgz.bak"; printf 'x' | dd of="$tgz" bs=1 seek=100 conv=notrunc status=none
if npm ci --offline >/dev/null 2>&1; then mv "$tgz.bak" "$tgz"; fail "npm ci accepted a tampered cache entry"; fi
mv "$tgz.bak" "$tgz"; pass "tampered cache entry rejected (integrity enforced)"
npm ci --offline >/dev/null 2>&1
else
  echo "  [SKIP] vendored-only bundle (no .npm-cache)"
fi

echo
echo "ALL CHECKS PASSED for $BUNDLE"
