mkdir -p "out/${CC}"
shopt -s nullglob
for f in work/${CC}/channels-${CC}-*.xml; do
  site="$(basename "$f" .xml | sed -E "s/^channels-${CC}-//")"

  echo "::group::Grab ${CC} / ${site}"
  echo "channels: $f"
  echo "site     : $site"

  set +e
  npm --prefix epg-src run grab --silent -- \
    --site "$site" \
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

# keep artifact step happy
if [[ -z "$(ls -A out/${CC} 2>/dev/null || true)" ]]; then
  echo "no site outputs for ${CC}" > "out/${CC}/README.txt"
fi
