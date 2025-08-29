// scripts/latam-iptvcat.mjs
// Crawl iptvcat LATAM pages → for each row follow the /my_list/... page in Playwright,
// extract ALL .m3u8 URLs, probe each, pick the best, and upsert to streams_latam.

import { chromium } from 'playwright';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { DateTime } from 'luxon';

// ---------- ENV ----------
const START_URL = process.env.IPTVCAT_START_URL || 'https://iptvcat.com/latin_america__7/';
const MAX_PAGES = Number(process.env.IPTVCAT_MAX_PAGES || '0'); // 0 = all
const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '10000');
const PROBE_CONCURRENCY = Number(process.env.PROBE_CONCURRENCY || '8');

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const STREAMS_TABLE = process.env.STREAMS_TABLE || 'streams_latam';

// ---------- DB ----------
function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

// ---------- Helpers ----------
async function withPage(browser, url, fn) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(30000);
  page.setDefaultTimeout(15000);
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(400);
    return await fn(page);
  } finally {
    await page.close();
  }
}

async function collectPagination(browser) {
  // Grab the first page and any numbered page links; keep unique and in order.
  const urls = await withPage(browser, START_URL, async (page) => {
    const base = new URL(page.url()).origin;
    const own = new URL(page.url()).href;
    const more = await page.$$eval('a[href]', (as) =>
      as
        .map((a) => a.getAttribute('href'))
        .filter(Boolean)
    );
    const set = new Set([own]);
    for (const h of more) {
      try {
        const u = new URL(h, location.href).href;
        if (/latin_america__7(\/\d+\/?)?$/.test(u)) set.add(u);
      } catch {}
    }
    return [...set];
  });
  return MAX_PAGES > 0 ? urls.slice(0, MAX_PAGES) : urls;
}

async function collectRowsFromPage(browser, url) {
  return withPage(browser, url, async (page) => {
    // Table shape can vary; be tolerant.
    return await page.$$eval('table tbody tr, tr', (rows) => {
      const out = [];
      for (const tr of rows) {
        const tds = [...tr.querySelectorAll('td')];
        // first td: channel name (often)
        const name = (tds[0]?.innerText || '').trim();
        // link to list downloader:
        const a = tr.querySelector('a[href*="/my_list/"]');
        if (!a || !name) continue;
        // quality is sometimes in the next column(s)
        const quality =
          (tds[1]?.innerText || '').trim() ||
          (tds[2]?.innerText || '').trim() ||
          '';
        // country often in last column
        const country = (tds[tds.length - 1]?.innerText || '').trim();
        out.push({
          channel_name: name,
          download_url: a.href,
          quality_hint: quality,
          country_hint: country,
        });
      }
      return out;
    });
  });
}

async function fetchListBodyViaBrowser(browser, url) {
  // /my_list/... may be protected; use browser context to render/serve the text.
  return await withPage(browser, url, async (page) => {
    // If it’s a text page, <pre> or body innerText typically contains URLs.
    const pre = await page.$('pre');
    if (pre) return (await pre.innerText()).trim();
    const bodyText = await page.evaluate(() => document.body?.innerText || '');
    return (bodyText || '').trim();
  });
}

function extractM3U8s(text) {
  const out = [];
  const re = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
  let m;
  while ((m = re.exec(text))) out.push(m[0]);
  return [...new Set(out)];
}

function absolutize(base, ref) {
  try {
    return new URL(ref, base).href;
  } catch {
    return ref;
  }
}

function bestFromMaster(baseUrl, text) {
  const lines = (text || '').split(/\r?\n/);
  const variants = [];
  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (L.startsWith('#EXT-X-STREAM-INF')) {
      const attrsRaw = (L.split(':')[1] || '').split(',');
      const attrs = Object.create(null);
      for (const kv of attrsRaw) {
        const [k, v] = kv.split('=');
        if (!k) continue;
        attrs[k.trim().toUpperCase()] = (v || '').trim().replace(/^"|"$/g, '');
      }
      const bw = Number(String(attrs.BANDWIDTH || '').replace(/[^0-9]/g, '')) || 0;
      const next = lines[i + 1] ? lines[i + 1].trim() : '';
      if (next && !next.startsWith('#')) variants.push({ bw, url: absolutize(baseUrl, next) });
    }
  }
  variants.sort((a, b) => b.bw - a.bw);
  return variants[0]?.url || null;
}

async function fetchText(url, timeoutMs = PROBE_TIMEOUT_MS) {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const r = await fetch(url, {
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/vnd.apple.mpegurl,text/plain,*/*' },
      redirect: 'follow',
      signal: ac.signal,
    });
    if (!r.ok) return null;
    return await r.text();
  } catch {
    return null;
  } finally {
    clearTimeout(to);
  }
}

async function probePlaylist(url) {
  // GET the content; if master, choose best variant; if media, just verify EXTM3U
  const txt = await fetchText(url);
  if (!txt) return { ok: false, url };

  if (/#EXT-X-STREAM-INF/i.test(txt)) {
    const best = bestFromMaster(url, txt) || url;
    const txt2 = await fetchText(best, 7000);
    if (!txt2) return { ok: false, url: best };
    return { ok: /#EXTM3U/.test(txt2), url: best };
  }

  return { ok: /#EXTM3U/.test(txt), url };
}

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

function rankByUrlHint(u) {
  // Lightweight tie-breaker when bandwidth info is absent
  if (/1080|fhd|fullhd|2k/i.test(u)) return 3;
  if (/720|hd/i.test(u)) return 2;
  return 1;
}

async function saveStreams(rows) {
  if (!rows.length) {
    console.log('No rows to save.');
    return;
  }
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('No Supabase creds; skipping DB upsert');
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

// ---------- MAIN ----------
async function main() {
  await fs.mkdir('out/latam', { recursive: true });
  const browser = await chromium.launch({ headless: HEADLESS });

  try {
    const pages = await collectPagination(browser);
    console.log('Pages to crawl:', pages.length);

    const allRows = [];
    for (const url of pages) {
      const rows = await collectRowsFromPage(browser, url);
      allRows.push(...rows);
    }
    // Dedup by download_url to avoid re-fetching the same list
    const unique = [...new Map(allRows.map((r) => [r.download_url, r])).values()];

    // For each row: open the my_list page in-browser, extract all .m3u8s
    const lists = [];
    for (const row of unique) {
      const body = await fetchListBodyViaBrowser(browser, row.download_url);
      const urls = extractM3U8s(body);
      lists.push({ ...row, m3u8s: urls });
    }
    await fs.writeFile(path.join('out', 'latam', 'iptvcat_lists.json'), JSON.stringify(lists, null, 2), 'utf8');

    // Probe all m3u8s with concurrency
    const probeTargets = [];
    for (const L of lists) {
      for (const u of L.m3u8s) probeTargets.push({ row: L, url: u });
    }

    const probed = await pLimit(PROBE_CONCURRENCY, probeTargets, async (t) => {
      const pr = await probePlaylist(t.url);
      return { channel_name: t.row.channel_name, country_hint: t.row.country_hint, quality_hint: t.row.quality_hint, download_url: t.row.download_url, candidate_url: pr.url, ok: pr.ok };
    });

    await fs.writeFile(path.join('out', 'latam', 'iptvcat_probes.json'), JSON.stringify(probed, null, 2), 'utf8');

    // Pick best working candidate per channel
    const byName = new Map();
    for (const p of probed) {
      if (!p.ok) continue;
      const key = p.channel_name.trim().toLowerCase();
      const candScore = 10 + rankByUrlHint(p.candidate_url);
      const cur = byName.get(key);
      if (!cur || candScore > cur.__score) byName.set(key, { ...p, __score: candScore });
    }
    const best = [...byName.values()];

    const now = DateTime.utc().toISO();
    const upserts = best.map((b) => ({
      stream_url: b.candidate_url,
      channel_name: b.channel_name,
      country_hint: b.country_hint || null,
      quality_hint: b.quality_hint || null,
      source_page_url: b.download_url,
      source: 'iptvcat',
      working: true,
      checked_at: now,
      extras: { origin: 'iptvcat' },
    }));

    await fs.writeFile(path.join('out', 'latam', 'streams_latam.json'), JSON.stringify(upserts, null, 2), 'utf8');
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
