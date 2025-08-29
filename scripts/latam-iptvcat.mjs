// scripts/latam-iptvcat.mjs
import { chromium } from 'playwright';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { DateTime } from 'luxon';

// ENV
const START_URL = process.env.IPTVCAT_START_URL || 'https://iptvcat.com/latin_america__7/';
const MAX_PAGES = Number(process.env.IPTVCAT_MAX_PAGES || '0');
const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '10000');
const PROBE_CONCURRENCY = Number(process.env.PROBE_CONCURRENCY || '8');

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const STREAMS_TABLE = process.env.STREAMS_TABLE || 'streams_latam';

// Supabase
function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

// Playwright helpers
async function newBrowser() {
  return chromium.launch({
    headless: HEADLESS,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
  });
}
async function newPage(browser) {
  const page = await browser.newPage({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  });
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  page.setDefaultNavigationTimeout(45000);
  page.setDefaultTimeout(20000);
  return page;
}
async function withPage(browser, url, fn) {
  const page = await newPage(browser);
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(500);
    return await fn(page);
  } finally {
    await page.close();
  }
}

// Crawl pagination (kept simple)
async function collectPagination(browser) {
  const urls = await withPage(browser, START_URL, async (page) => {
    const own = new URL(page.url()).href;
    const set = new Set([own]);
    const links = await page.$$eval('a[href]', (as) =>
      as.map((a) => a.getAttribute('href')).filter(Boolean)
    );
    for (const href of links) {
      try {
        const u = new URL(href, location.href).href;
        // Accept current page and numbered sub-pages like /latin_america__7/2/
        if (/latin_america__7(\/\d+\/?)?$/i.test(u)) set.add(u);
      } catch {}
    }
    return [...set];
  });
  return MAX_PAGES > 0 ? urls.slice(0, MAX_PAGES) : urls;
}

// Extract rows from a listing page
async function collectRowsFromPage(browser, url) {
  return withPage(browser, url, async (page) => {
    return await page.$$eval('table tbody tr, tr', (rows) => {
      const out = [];
      for (const tr of rows) {
        const tds = [...tr.querySelectorAll('td')];
        const name = (tds[0]?.innerText || '').trim();
        const dl = tr.querySelector('a[href*="/my_list/"]');
        const direct = [...tr.querySelectorAll('a[href$=".m3u8"]')].map((a) => a.href);
        if (!name || (!dl && direct.length === 0)) continue;
        const quality = (tds[1]?.innerText || tds[2]?.innerText || '').trim();
        const country = (tds[tds.length - 1]?.innerText || '').trim();
        out.push({
          channel_name: name,
          download_url: dl ? dl.href : null,
          direct_m3u8s: direct,
          quality_hint: quality,
          country_hint: country,
          source_page: location.href,
        });
      }
      return out;
    });
  });
}

// ----- NEW: fetch list bodies without navigating (avoids download error) -----
async function fetchListBody(url, referer) {
  try {
    const r = await fetch(url, {
      redirect: 'follow',
      headers: {
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
        accept: 'text/plain,*/*',
        referer: referer || 'https://iptvcat.com/',
      },
    });
    if (!r.ok) return '';
    const ct = r.headers.get('content-type') || '';
    // Both text/plain and application/x-mpegURL etc are fine
    if (!/text|mpegurl|application\/x-mpegurl/i.test(ct)) {
      // still try to read as text; if it fails, return empty
    }
    return await r.text();
  } catch {
    return '';
  }
}

function extractM3U8s(text) {
  const out = [];
  const re = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
  let m;
  while ((m = re.exec(text))) out.push(m[0]);
  return [...new Set(out)];
}

// Parse master playlist to pick best variant
function bestFromMaster(baseUrl, txt) {
  const lines = (txt || '').split(/\r?\n/);
  const vars = [];
  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L.startsWith('#EXT-X-STREAM-INF')) continue;
    const attrs = (L.split(':')[1] || '')
      .split(',')
      .reduce((a, p) => {
        const [k, v] = (p || '').split('=');
        if (k) a[k.trim().toUpperCase()] = (v || '').trim().replace(/^"|"$/g, '');
        return a;
      }, {});
    const bw = Number(String(attrs.BANDWIDTH || '').replace(/[^0-9]/g, '')) || 0;
    const next = lines[i + 1]?.trim() || '';
    if (next && !next.startsWith('#')) {
      try {
        vars.push({ bw, url: new URL(next, baseUrl).href });
      } catch {
        vars.push({ bw, url: next });
      }
    }
  }
  vars.sort((a, b) => b.bw - a.bw);
  return vars[0]?.url || null;
}

// Probe a candidate URL
async function fetchText(url, signal) {
  try {
    const r = await fetch(url, {
      headers: {
        'user-agent': 'Mozilla/5.0',
        accept: 'application/vnd.apple.mpegurl,text/plain,*/*',
      },
      redirect: 'follow',
      signal,
    });
    const t = await r.text();
    return { ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '', url: r.url, txt: t };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}
async function probePlaylist(url) {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), PROBE_TIMEOUT_MS);
  try {
    let r = await fetchText(url, ac.signal);
    if (r.ok && /#EXTM3U/.test(r.txt)) {
      if (/#EXT-X-STREAM-INF/i.test(r.txt)) {
        const best = bestFromMaster(url, r.txt) || url;
        const r2 = await fetchText(best, ac.signal);
        return { ok: r2.ok && /#EXTM3U/.test(r2.txt), url: r2.url || best, reason: 'master-best' };
      }
      return { ok: true, url: r.url || url, reason: 'media-extm3u' };
    }
    if (r.ok && /mpegurl|application\/x-mpegURL/i.test(r.ct)) return { ok: true, url: r.url || url, reason: 'ct-hls' };
    return { ok: false, url, reason: 'no-extm3u' };
  } finally {
    clearTimeout(to);
  }
}

// Concurrency
async function pLimit(n, arr, fn) {
  const out = new Array(arr.length);
  let i = 0;
  const workers = Array.from({ length: n }, async () => {
    while (i < arr.length) {
      const idx = i++;
      out[idx] = await fn(arr[idx], idx);
    }
  });
  await Promise.all(workers);
  return out;
}

// Rank
function rankByHint(u, q) {
  const urlScore = /2160|4k|uhd/i.test(u) ? 4 : /1080|fhd/i.test(u) ? 3 : /720|hd/i.test(u) ? 2 : 1;
  const qual = /2160|4k|uhd/i.test(q || '')
    ? 4
    : /1080|fhd/i.test(q || '')
    ? 3
    : /720|hd/i.test(q || '')
    ? 2
    : 1;
  return urlScore * 10 + qual;
}

// Save to Supabase
async function saveStreams(rows) {
  if (!rows.length) {
    console.log('No rows to save.');
    return;
  }
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('No Supabase creds; skip');
    return;
  }
  const client = sb();
  const BATCH = 500;
  for (let i = 0; i < rows.length; i += BATCH) {
    const slice = rows.slice(i, i + BATCH);
    const { error } = await client.from(STREAMS_TABLE).upsert(slice, { onConflict: 'stream_url' });
    if (error) {
      console.warn('Upsert error:', error.message);
      break;
    }
  }
  console.log(`Saved ${rows.length} rows to ${STREAMS_TABLE}`);
}

async function main() {
  await fs.mkdir('out/latam', { recursive: true });
  const browser = await newBrowser();
  try {
    const pages = await collectPagination(browser);
    console.log('Pages to crawl:', pages.length);
    await fs.writeFile(path.join('out', 'latam', 'pages.json'), JSON.stringify(pages, null, 2), 'utf8');

    const all = [];
    for (const url of pages) {
      const rows = await collectRowsFromPage(browser, url);
      all.push(...rows);
    }
    await fs.writeFile(path.join('out', 'latam', 'rows_raw.json'), JSON.stringify(all, null, 2), 'utf8');

    // De-dupe by unique download url or direct set
    const map = new Map();
    for (const r of all) {
      const key = r.download_url || `direct:${r.channel_name}:${(r.direct_m3u8s || []).join('|')}`;
      if (!map.has(key)) map.set(key, r);
    }
    const unique = [...map.values()];

    // Expand: fetch the "my_list" text via fetch (NOT Playwright navigation)
    const expanded = [];
    for (const row of unique) {
      let urls = [...(row.direct_m3u8s || [])];
      if (row.download_url) {
        const body = await fetchListBody(row.download_url, row.source_page);
        urls.push(...extractM3U8s(body));
      }
      urls = [...new Set(urls)];
      expanded.push({ ...row, m3u8s: urls });
    }
    await fs.writeFile(
      path.join('out', 'latam', 'iptvcat_lists.json'),
      JSON.stringify(expanded, null, 2),
      'utf8'
    );

    const targets = [];
    for (const L of expanded) for (const u of L.m3u8s) targets.push({ row: L, url: u });
    await fs.writeFile(
      path.join('out', 'latam', 'probe_targets.json'),
      JSON.stringify({ count: targets.length }, null, 2),
      'utf8'
    );

    const probed = await pLimit(PROBE_CONCURRENCY, targets, async (t) => {
      const pr = await probePlaylist(t.url);
      return {
        channel_name: t.row.channel_name,
        country_hint: t.row.country_hint,
        quality_hint: t.row.quality_hint,
        source_page_url: t.row.source_page,
        download_url: t.row.download_url,
        candidate_url: pr.url,
        ok: pr.ok,
        reason: pr.reason,
      };
    });
    await fs.writeFile(
      path.join('out', 'latam', 'iptvcat_probes.json'),
      JSON.stringify(probed, null, 2),
      'utf8'
    );

    // Pick best per channel
    const byName = new Map();
    for (const p of probed) {
      if (!p.ok) continue;
      const key = p.channel_name.trim().toLowerCase();
      const score = rankByHint(p.candidate_url, p.quality_hint);
      const cur = byName.get(key);
      if (!cur || score > cur.__score) byName.set(key, { ...p, __score: score });
    }
    const best = [...byName.values()];

    const now = DateTime.utc().toISO();
    const upserts = best.map((b) => ({
      stream_url: b.candidate_url,
      channel_name: b.channel_name,
      country_hint: b.country_hint || null,
      quality_hint: b.quality_hint || null,
      source_page_url: b.download_url || b.source_page_url || null,
      source: 'iptvcat',
      working: true,
      checked_at: now,
      extras: { origin: 'iptvcat', reason: b.reason },
    }));
    await fs.writeFile(
      path.join('out', 'latam', 'streams_latam.json'),
      JSON.stringify(upserts, null, 2),
      'utf8'
    );

    await saveStreams(upserts);
    console.log(`iptvcat ingestion done. Channels kept: ${upserts.length}`);
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});