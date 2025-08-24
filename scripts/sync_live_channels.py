#!/usr/bin/env python3
import os, sys, re, json, ssl, tempfile, pathlib
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from collections import Counter
from typing import Dict, Any, List, Optional

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
COUNTRIES    = [c.strip().upper() for c in os.environ.get("COUNTRIES", "PR,DE,US,ES,MX,IT,IE,CA,AU,UK").split(",") if c.strip()]

SEARCH_BASE   = "https://iptv-org.github.io/?q=live%20country:{cc}"
CHANNELS_JSON = "https://iptv-org.github.io/api/channels.json"
STREAMS_JSON  = "https://iptv-org.github.io/api/streams.json"  # URL→channel id mapping when tvg-id is missing

def require_env():
    miss = []
    if not SUPABASE_URL: miss.append("SUPABASE_URL")
    if not SERVICE_KEY:  miss.append("SUPABASE_SERVICE_ROLE_KEY")
    if miss: raise SystemExit(f"Missing env: {', '.join(miss)}")
    dom = SUPABASE_URL.split("//",1)[1].split("/",1)[0]
    print("ENV OK:", dom)
    print("COUNTRIES:", COUNTRIES)

def fetch_text(url: str, timeout=90) -> str:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "iptv-live-html/1.3"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return resp.read().decode("utf-8", "ignore")

def fetch_json(url: str, timeout=90) -> Any:
    return json.loads(fetch_text(url, timeout=timeout))

def absolutize(href: str) -> str:
    if not href: return ""
    if href.startswith(("http://","https://")): return href
    if href.startswith("/"): return "https://iptv-org.github.io" + href
    return "https://iptv-org.github.io/" + href

def playlist_text_from_search(country: str) -> str:
    """
    Open the search page, then:
      1) Try to capture any link with '.m3u' (preferring ones that include the query).
      2) Click the Feed control and capture:
         - a download (page.expect_download), OR
         - any network response whose URL ends with '.m3u'.
      3) Fallback: try constructed index.m3u with encoded query (as a last resort).
    """
    from playwright.sync_api import sync_playwright
    url = SEARCH_BASE.format(cc=country)
    query_tokens = [
        f"q=live%20country:{country}",
        f"q=live+country:{country}",
        f"q=live%20country%3A{country}",
        f"q=live+country%3A{country}",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (LiveScraper/1.3)")

        # --- helper to fetch arbitrary URL via page (to reuse UA / cookies if any)
        def pull_url(u: str) -> Optional[str]:
            try:
                resp = page.request.get(u, timeout=60_000)
                if resp.ok:
                    return resp.text()
            except Exception:
                pass
            return None

        try:
            page.goto(url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1500)

            # (A) Look for any visible <a> with .m3u
            hrefs: List[str] = page.eval_on_selector_all(
                "a[href]", "els => els.map(e => e.getAttribute('href'))"
            ) or []
            m3u_hrefs = [absolutize(h) for h in hrefs if h and ".m3u" in h.lower()]

            # Prefer link that includes our query tokens
            for h in m3u_hrefs:
                if any(tok.lower() in h.lower() for tok in query_tokens):
                    txt = pull_url(h)
                    if txt and txt.strip().startswith("#EXTM3U"):
                        print(f"[{country}] Found m3u via anchor: {h}")
                        return txt

            # (B) Click a “Feed” control and capture download / response
            m3u_from_response = {"url": None, "body": None}

            def on_response(resp):
                url = resp.url
                if ".m3u" in url.lower():
                    try:
                        body = resp.text()
                        if body and body.strip().startswith("#EXTM3U"):
                            m3u_from_response["url"]  = url
                            m3u_from_response["body"] = body
                    except Exception:
                        pass

            page.on("response", on_response)

            # Possible selectors for the Feed control
            candidates = [
                'a[aria-label="Feed"]',
                'a[aria-label="feed"]',
                'a:has-text("Feed")',
                'button:has-text("Feed")',
                '[title~="Feed"]',
                '[data-tooltip~="Feed"]',
            ]
            clicked = False
            for sel in candidates:
                loc = page.locator(sel)
                if loc.count() > 0:
                    # Try download capture
                    try:
                        with page.expect_download(timeout=5_000) as dlinfo:
                            loc.first.click()
                        dl = dlinfo.value
                        path = dl.path() or (lambda: (dl.save_as(tmp := str(pathlib.Path(tempfile.gettempdir()) / dl.suggested_filename)), tmp))()
                        # read file content
                        m3u_text = pathlib.Path(path).read_text(encoding="utf-8", errors="ignore")
                        if m3u_text.strip().startswith("#EXTM3U"):
                            print(f"[{country}] Captured m3u via download: {dl.suggested_filename}")
                            return m3u_text
                    except Exception:
                        # No download? still might have a network response
                        try:
                            loc.first.click()
                        except Exception:
                            pass
                    clicked = True
                    page.wait_for_timeout(800)
                    if m3u_from_response["body"]:
                        print(f"[{country}] Captured m3u via network response: {m3u_from_response['url']}")
                        return m3u_from_response["body"]

            # (C) Last resort: try a constructed feed URL for the current query
            # (These forms are what the site typically uses when sharing Feed URLs)
            guesses = [
                f"https://iptv-org.github.io/iptv/index.m3u?q=live%20country%3A{country}",
                f"https://iptv-org.github.io/iptv/index.m3u?q=live+country%3A{country}",
                f"https://iptv-org.github.io/iptv/index.m3u?q=live%20country:{country}",
                f"https://iptv-org.github.io/iptv/index.m3u?q=live+country:{country}",
            ]
            for g in guesses:
                txt = pull_url(g)
                if txt and txt.strip().startswith("#EXTM3U"):
                    print(f"[{country}] Fallback constructed feed worked: {g}")
                    return txt

            raise RuntimeError("No .m3u link found or generated from the search page")
        finally:
            browser.close()

# --- Supabase helpers ------------------------------------------------

def supabase_delete_country(cc: str):
    import urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels?country=eq.{cc}"
    req = urllib.request.Request(
        url=url, method="DELETE",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Prefer": "return=minimal"
        }
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status not in (200,204):
            raise RuntimeError(f"DELETE {cc} failed: {resp.status}")

def upsert_rows(rows: List[Dict[str, Any]]):
    import urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels?on_conflict=channel_id"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(
        url=url, data=data, method="POST",
        headers={
            "Content-Type": "application/json",
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Prefer": "resolution=merge-duplicates"
        },
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status not in (200,201,204):
            body = resp.read().decode("utf-8", "ignore")
            raise RuntimeError(f"Supabase upsert failed {resp.status}: {body}")

# --- M3U parsing -----------------------------------------------------

ATTR_RE = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')

def parse_m3u(text: str) -> List[Dict[str, str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF"):
            try:
                header, display = line.split(",", 1)
            except ValueError:
                header, display = line, ""
            attrs = dict(ATTR_RE.findall(header))
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            url = lines[j] if j < len(lines) else None
            if url:
                out.append({
                    "tvg_id"  : (attrs.get("tvg-id") or "").strip().lower(),
                    "tvg_name": (attrs.get("tvg-name") or "").strip(),
                    "tvg_logo": (attrs.get("tvg-logo") or "").strip(),
                    "display" : display.strip(),
                    "url"     : url.strip(),
                })
            i = j + 1
        else:
            i += 1
    return out

# --- Country code helpers -------------------------------------------

def catalog_match_code(cc: str) -> str:
    """Map display country to catalog country (GB vs UK)."""
    return "GB" if cc == "UK" else cc

def stored_country(cc: str) -> str:
    """Normalize for storage (we keep 'UK' as display)."""
    return "UK" if cc in ("UK","GB") else cc

# --- Main ------------------------------------------------------------

def main():
    require_env()

    # Canonical channel catalog
    catalog = fetch_json(CHANNELS_JSON)
    by_id = { (c.get("id") or "").lower(): c for c in catalog if c.get("id") }

    # URL→channel ID mapping (for rows lacking tvg-id)
    streams = fetch_json(STREAMS_JSON)
    url_to_id = {}
    for s in streams:
        cid = (s.get("channel") or "").lower()
        url = (s.get("url") or "").strip()
        if cid and url:
            url_to_id[url] = cid

    final_rows: List[Dict[str, Any]] = []
    kept_by_cc = Counter()

    for cc in COUNTRIES:
        print(f"[{cc}] Deleting existing rows in Supabase …")
        supabase_delete_country(cc)

        print(f"[{cc}] Getting Feed playlist (m3u) …")
        m3u_text = playlist_text_from_search(cc)
        entries = parse_m3u(m3u_text)
        print(f"[{cc}] Parsed entries: {len(entries)}")

        rows_by_id: Dict[str, Dict[str, Any]] = {}
        now_iso = datetime.now(timezone.utc).isoformat()
        want_cc = catalog_match_code(cc)

        for e in entries:
            tvg_id   = e["tvg_id"]
            tvg_name = e["tvg_name"]
            tvg_logo = e["tvg_logo"]
            url      = e["url"]

            channel_id = None
            if tvg_id and tvg_id in by_id:
                channel_id = tvg_id
            elif url in url_to_id and url_to_id[url] in by_id:
                channel_id = url_to_id[url]
            else:
                continue

            meta = by_id[channel_id]
            meta_cc = (meta.get("country") or "").upper()
            # require country match (GB <-> UK treated as same)
            if not ({meta_cc, want_cc} & {"GB","UK"}) and meta_cc != want_cc:
                continue

            if channel_id not in rows_by_id:
                rows_by_id[channel_id] = {
                    "channel_id":   channel_id,
                    "display_name": meta.get("name") or tvg_name or channel_id,
                    "country":      stored_country(cc),
                    "icon_url":     meta.get("logo") or (tvg_logo or None),
                    "stream_url":   url,
                    "check_time":   now_iso,
                    "source":       "iptv-org-html",
                    "updated_at":   now_iso,
                }

        rows = list(rows_by_id.values())
        print(f"[{cc}] Kept (deduped): {len(rows)}")

        if rows:
            upsert_rows(rows)
            kept_by_cc[cc] = len(rows)
            final_rows.extend(rows)
        else:
            kept_by_cc[cc] = 0

    print("Summary kept_by_country:", dict(kept_by_cc))
    print("Total upserted:", len(final_rows))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr); sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr); sys.exit(1)
