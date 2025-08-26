name: EPG-MX

on:
  workflow_dispatch: {}
  schedule:
    - cron: '0 7 * * *'

concurrency:
  group: epg-mx
  cancel-in-progress: true

jobs:
  mx:
    runs-on: ubuntu-latest
    timeout-minutes: 300
    env:
      MX_SEARCH_URL: https://iptv-org.github.io/?q=live%20country:MX
      MX_EPG_URL: https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz
      HEADLESS: 'true'
      MAX_CHANNELS: '0'
      PER_PAGE_DELAY_MS: '150'
      NAV_TIMEOUT_MS: '30000'
      PROBE_TIMEOUT_MS: '5000'
      FUZZY_MIN: '0.45'
      LOG_UNMATCHED: '1'
      SUPABASE_SCHEMA: public
      SUPABASE_TABLE: mx_channels
      SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
      SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
      # Put the full JS into a multi-line env var to avoid heredoc parsing issues
      MX_SCRIPT: |
        <<PASTE THE FULL CONTENTS OF scripts/mx-scrape-and-match.mjs FROM ABOVE HERE>>

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Install Playwright and deps
        run: |
          npm i --no-save playwright saxes @supabase/supabase-js
          npx playwright install --with-deps chromium

      - name: Write script
        shell: bash
        run: |
          set -euo pipefail
          mkdir -p scripts out/mx
          printf '%s\n' "$MX_SCRIPT" > scripts/mx-scrape-and-match.mjs
          # sanity: ensure old concat symbol not present
          if grep -n 'dispBuf' scripts/mx-scrape-and-match.mjs; then
            echo "Found legacy dispBuf in script (should not exist)"; exit 1
          fi

      - name: Run MX scrape & match
        run: node scripts/mx-scrape-and-match.mjs

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: mx-output
          path: |
            out/mx/records.json
            out/mx/matches.json
            out/mx/unmatched.json
          if-no-files-found: ignore
