#!/usr/bin/env bash
# make-offline-bundle.sh — build a self-contained valkey-polyglot-benchmark.tar.gz whose Node
# benchmark installs and runs with ZERO npm registry access on the target host (air-gapped
# hosts, regions behind lagging npm mirrors, CI with no egress).
#
# Output layout (the repository tree, minus .git, plus a hermetic node/):
#
#   valkey-polyglot-benchmark/
#     node/
#       package.json            (@valkey/valkey-glide pinned to the exact resolved version)
#       package-lock.json       (regenerated, in sync, sha512 integrity for every dependency)
#       .npmrc                  (offline=true, cache=.npm-cache, audit/fund off, ignore-scripts)
#       .npm-cache/             (npm cache: every tarball for linux x64 + arm64 glibc)
#       node_modules/           (pre-installed for the build host's linux arch, ready to run)
#       BUNDLE-MANIFEST.txt     (source commit, versions, per-package integrity)
#     <other directories unchanged>
#
# On the target host all of these work without a registry:
#   tar -xzf valkey-polyglot-benchmark.tar.gz && cd valkey-polyglot-benchmark/node
#   node valkey-benchmark.js --help          # node_modules is already there
#   npm install                              # no-op, resolved from .npm-cache
#   rm -rf node_modules && npm ci --offline  # full reinstall from .npm-cache, integrity-checked
#
# Usage:
#   node/scripts/make-offline-bundle.sh [-s <repo checkout>] [-o <out.tar.gz>] [-g <glide version>]
#                                       [-r <registry>] [-c "<cpus>"] [-m full|cache|vendored]
#   -s  source checkout            (default: the repository this script lives in)
#   -o  output tarball             (default: <repo>/dist/valkey-polyglot-benchmark.tar.gz, gitignored)
#   -g  pin @valkey/valkey-glide   (default: whatever package.json's range resolves to)
#   -r  registry used at build time (default: https://registry.npmjs.org)
#   -c  native variants to cache    (default: "x64 arm64", linux glibc)
#   -m full      (default) .npm-cache + node_modules  (~39 MB; works with npm install, npm ci, or no npm)
#      cache     .npm-cache only                      (~25 MB; npm install / npm ci reinstall offline)
#      vendored  node_modules only                    (~14 MB; npm install is a no-op; build-host arch only)
#
# Network to the registry is needed ONLY on the machine running this script. The output is
# byte-reproducible for a given source commit and registry content.
set -euo pipefail

# When invoked through `npm run`, the parent npm exports its own configuration as npm_config_*
# environment variables (cache, prefix, registry, ...). Environment beats the project .npmrc,
# which would silently redirect the cache. Start from a clean npm configuration.
for v in $(env | sed -n 's/^\(npm_config_[^=]*\)=.*/\1/p'); do unset "$v"; done
unset npm_package_json npm_lifecycle_event npm_lifecycle_script npm_execpath npm_node_execpath 2>/dev/null || true

SRC="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$SRC/dist/valkey-polyglot-benchmark.tar.gz"
GLIDE_VERSION=""           # empty = keep package.json range, just sync the lock
REGISTRY="https://registry.npmjs.org"
CPUS="x64 arm64"           # native-binary variants to pre-cache (linux/glibc)
MODE="full"

while getopts "s:o:g:r:c:m:h" opt; do
  case "$opt" in
    m) MODE="$OPTARG"; case "$MODE" in full|cache|vendored) ;; *) echo "bad -m $MODE" >&2; exit 2;; esac ;;
    s) SRC="$(cd "$OPTARG" && pwd)" ;;
    o) OUT="$OPTARG" ;;
    g) GLIDE_VERSION="$OPTARG" ;;
    r) REGISTRY="$OPTARG" ;;
    c) CPUS="$OPTARG" ;;
    h) sed -n '2,40p' "$0"; exit 0 ;;
    *) exit 2 ;;
  esac
done

case "$OUT" in /*) ;; *) OUT="$(pwd)/$OUT" ;; esac   # absolute: we cd around below
[ -f "$SRC/node/package.json" ] || { echo "no node/package.json under $SRC" >&2; exit 1; }
command -v node >/dev/null && command -v npm >/dev/null || { echo "node/npm required" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
STAGE="$WORK/valkey-polyglot-benchmark"
mkdir -p "$STAGE"

echo "==> staging $SRC (dropping .git, .claude, node_modules)"
tar -C "$SRC" --exclude=./.git --exclude=./.claude --exclude=./dist --exclude=./node/node_modules --exclude=./node/.npm-cache -cf - . | tar -C "$STAGE" -xf -
SOURCE_COMMIT="$(git -C "$SRC" rev-parse HEAD 2>/dev/null || echo unknown)"

NODE_DIR="$STAGE/node"
cd "$NODE_DIR"
rm -f .npmrc

# Build-time npm config: talk to $REGISTRY and keep packuments out of the bundle. protobufjs
# has a postinstall; the vendored tree is produced with scripts ON below so it is complete,
# and the runtime .npmrc sets ignore-scripts so nothing runs on the target host.
export npm_config_registry="$REGISTRY"
export npm_config_cache="$WORK/resolve-cache"   # packuments go here, NOT into the bundle
export npm_config_audit=false npm_config_fund=false npm_config_update_notifier=false
export npm_config_loglevel=warn

if [ -n "$GLIDE_VERSION" ]; then
  echo "==> pinning @valkey/valkey-glide to exactly $GLIDE_VERSION"
  npm pkg set "dependencies.@valkey/valkey-glide=$GLIDE_VERSION"
fi

echo "==> regenerating package-lock.json in sync with package.json"
rm -f package-lock.json
npm install --package-lock-only --ignore-scripts

RESOLVED_GLIDE="$(node -p "require('./package-lock.json').packages['node_modules/@valkey/valkey-glide'].version")"
echo "    resolved @valkey/valkey-glide@$RESOLVED_GLIDE"

# Freeze the exact resolved version in package.json too, so a later `npm install` on a
# host can never re-resolve it, even if someone deletes the lock.
npm pkg set "dependencies.@valkey/valkey-glide=$RESOLVED_GLIDE"
npm install --package-lock-only --ignore-scripts   # re-sync after the pin (no-op resolution)

echo "==> populating .npm-cache for linux glibc: $CPUS (and installing node_modules)"
export npm_config_cache="$NODE_DIR/.npm-cache"   # npm ci is lock-driven: only tarballs are fetched
for cpu in $CPUS; do
  rm -rf node_modules
  # Real install (scripts on) so the vendored tree is exactly what npm would produce.
  npm ci --omit=dev --os=linux --cpu="$cpu" --foreground-scripts
done
# Leave node_modules for the build host's own arch.
HOST_CPU="$(node -p process.arch)"
rm -rf node_modules
npm ci --omit=dev --os=linux --cpu="$HOST_CPU" --foreground-scripts
npm cache verify >/dev/null

echo "==> writing runtime .npmrc (hermetic npm on the target host)"
cat > .npmrc <<'EOF'
; Written by node/scripts/make-offline-bundle.sh. Makes `npm install` / `npm ci` in this directory
; hermetic: everything is resolved from ./.npm-cache and verified against the sha512
; integrity in package-lock.json. No registry is ever contacted.
offline=true
prefer-offline=true
cache=.npm-cache
audit=false
fund=false
update-notifier=false
ignore-scripts=true
loglevel=warn
EOF

echo "==> writing BUNDLE-MANIFEST.txt"
{
  echo "source-commit: $SOURCE_COMMIT"
  echo "built-with: node $(node --version), npm $(npm --version), $(uname -s)/$(uname -m)"
  echo "registry-used-at-build: $REGISTRY"
  echo "native-variants-cached: linux/{$(echo $CPUS | tr ' ' ',')} glibc"
  echo "node_modules-installed-for: linux/$HOST_CPU"
  echo "valkey-glide: $RESOLVED_GLIDE"
  echo
  echo "# name version integrity"
  node -e '
    const p = require("./package-lock.json").packages;
    for (const k of Object.keys(p).sort()) {
      if (!k) continue;
      const v = p[k];
      console.log(`${k.replace(/^.*node_modules\//, "")} ${v.version} ${v.integrity || "-"}${v.optional ? " optional" : ""}${v.os ? " os=" + v.os : ""}${v.cpu ? " cpu=" + v.cpu : ""}`);
    }'
} > BUNDLE-MANIFEST.txt

# Make the cache byte-reproducible: content-v2 is content-addressed already; index-v5 entries
# embed fetch timestamps and volatile HTTP response headers. Keep one entry per key, pin the
# timestamps to the upstream commit time, keep only the headers npm needs, and re-hash.
rm -rf .npm-cache/_logs .npm-cache/_update-notifier-last-checked .npm-cache/_cacache/tmp .npm-cache/_cacache/_lastverified
SRC_EPOCH="$(git -C "$SRC" log -1 --format=%ct 2>/dev/null || echo 1700000000)"
node - "$NODE_DIR/.npm-cache/_cacache/index-v5" "$SRC_EPOCH" <<'EOJS'
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const [root, epoch] = process.argv.slice(2);
const KEEP = new Set(["content-type", "content-length", "etag", "last-modified", "cache-control"]);
const walk = d => fs.readdirSync(d, { withFileTypes: true }).flatMap(e => e.isDirectory() ? walk(path.join(d, e.name)) : [path.join(d, e.name)]);
for (const f of walk(root)) {
  const byKey = new Map();
  for (const line of fs.readFileSync(f, "utf8").split("\n")) {
    const tab = line.indexOf("\t"); if (tab < 0) continue;
    const e = JSON.parse(line.slice(tab + 1));
    if (!e.integrity) continue;                 // tombstone
    byKey.set(e.key, e);                         // last write wins
  }
  const out = [];
  for (const e of [...byKey.values()].sort((a, b) => a.key < b.key ? -1 : 1)) {
    const m = e.metadata || {};
    const resHeaders = Object.fromEntries(Object.entries(m.resHeaders || {}).filter(([k]) => KEEP.has(k)).sort());
    const metadata = { time: Number(epoch) * 1000, url: m.url, reqHeaders: {}, resHeaders, options: m.options };
    const norm = { key: e.key, integrity: e.integrity, time: Number(epoch) * 1000, size: e.size, metadata };
    const str = JSON.stringify(norm);
    out.push(crypto.createHash("sha1").update(str).digest("hex") + "\t" + str);
  }
  fs.writeFileSync(f, "\n" + out.join("\n") + "\n");
}
EOJS
npm cache verify >/dev/null   # proves the rewritten index is still valid
rm -rf .npm-cache/_cacache/_lastverified .npm-cache/_cacache/tmp .npm-cache/_logs

case "$MODE" in
  cache)    echo "==> mode=cache: dropping node_modules";  rm -rf node_modules ;;
  vendored) echo "==> mode=vendored: dropping .npm-cache"; rm -rf .npm-cache; sed -i '/^cache=/d; /^offline=/d; /^prefer-offline=/d' .npmrc ;;
esac
echo "mode: $MODE" >> BUNDLE-MANIFEST.txt

echo "==> packing reproducible $OUT"
mkdir -p "$(dirname "$OUT")"
( cd "$WORK" && tar --sort=name --mtime="@$SRC_EPOCH" --owner=0 --group=0 --numeric-owner \
     -cf - valkey-polyglot-benchmark ) | gzip -n -9 > "$OUT"

echo
echo "bundle: $OUT ($(du -h "$OUT" | cut -f1))"
echo "sha256: $(sha256sum "$OUT" | cut -d' ' -f1)"
echo "glide : $RESOLVED_GLIDE   source: $SOURCE_COMMIT"
du -sh "$NODE_DIR"/.npm-cache "$NODE_DIR"/node_modules 2>/dev/null | sed 's/^/  /' || true
