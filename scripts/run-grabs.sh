#!/usr/bin/env bash
set -euo pipefail

IFS=',' read -ra CCS <<< "${COUNTRIES}"

mkdir -p out

for CC in "${CCS[@]}"; do
  CC="$(echo "$CC" | tr '[:lower:]' '[:upper:]')"
  export CC
  echo "== Prepare channels for ${CC} =="

  node scripts/make-channels-by-site.mjs

  mkdir -p "out/${CC}"

  shopt -s nullglob
  for f in work/${CC}/channels-${CC}-*.xml; do
    site="$(basename "$f" .xml | sed -E "s/^channels-${CC}-//")"
    cfg="epg-src/sites/${site}/${site}.config.js"

    if [[ ! -f "$cfg" ]]; then
      echo "::warning ::No config for site '${site}', skipping (${cfg})"
      continue
    fi

    echo "::group::Grab ${CC} / ${site}"
    echo "channels: $f"
    echo "config   : $cfg"

    # Use iptv-org repo's script (it wraps epg-grabber properly)
    # Keep it gentle to avoid rate-limits and OOM
    set +e
    npm --prefix epg-src run grab --silent -- \
      --config "$cfg" \
      --channels "$f" \
      --output "out/${CC}/${site}.xml" \
      --days 1 \
      --maxConnections 2 \
      --concurrency 2 \
      --timeout 180000 \
      --delay 500 \
      --debug 2>&1 | tee "out/${CC}/${site}.log"
    rc=${PIPESTATUS[0]}
    set -e

    if [[ -f "out/${CC}/${site}.xml" ]]; then
      gzip -f "out/${CC}/${site}.xml"
      echo "Wrote out/${CC}/${site}.xml.gz"
    else
      echo "::notice ::No XML produced for ${site} (${CC}). See log out/${CC}/${site}.log"
    fi
    echo "::endgroup::"
  done

  # Keep artifact step happy even if empty
  if [[ -z "$(ls -A out/${CC} 2>/dev/null || true)" ]]; then
    echo "no site outputs for ${CC}" > "out/${CC}/README.txt"
  fi
done
