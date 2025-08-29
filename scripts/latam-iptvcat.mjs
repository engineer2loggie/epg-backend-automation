// scripts/latam-iptvcat.mjs
// Crawl iptvcat LATAM pages → follow each /my_list/... page in-browser,
// extract ALL .m3u8 URLs (plus any direct table .m3u8 links), probe them,
// pick the best per channel, upsert to streams_latam, and emit rich artifacts.

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

// ---------- Playwright helpers ----------
async function newBrowser() {
  return chromium.launch({
    headless: HEADLESS,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--no-sandbox',
      '--disable-gpu',
    ],
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
    await page.waitForTimeout(900);
    return await fn(page);
  } finally {
    await page.close();
  }
}

// ---------- Crawl pagination ----------
async function collectPagination(browser) {
  const urls = await withPage(browser, START_URL, async (page) => {
    const own = new URL(page.url()).href;
    const list = new Set([own]);
    const links = await page.$$eval('a[href]', (as) =>
      as.map((a) => a.getAttribute('href')).filter(Boolean)
    );
    for (const href of links) {
      try {
        const url = new URL(href, location.href).href;
        if (/latin_america__7(\/\d+\/?)?$/.test(url)) list.add(url);
      } catch {}
    }
    return [...list];
  });
  return MAX_PAGES > 0 ? urls.slice(0, MAX_PAGES) : urls;
}

// ---------- Extract rows from a list page ----------
async function collectRowsFromPage(browser, url) {
  return withPage(browser, url, async (page) => {
    return await page.$$eval('table tbody tr, tr', (rows) => {
      const out = [];
      for (const tr of rows) {
        const tds = [...tr.querySelectorAll('td')];
        const name = (tds[0]?.innerText || '').trim();
        const dl = tr.querySelector('a[href*="/my_list/"]');
        const directM3U8s = [...tr.querySelectorAll('a[href$=".m3u8"]')].map((a) => a.href);
        if (!name || (!dl && directM3U8s.length === 0)) continue;

        const quality =
          (tds[1]?.innerText || '').trim() ||
          (tds[2]?.innerText || '').trim() ||
          '';
        const country = (tds[tds.length - 1]?.innerText || '').trim();

        out.push({
          channel_name: name,
          download_url: dl ? dl.href : null,
          direct_m3u8s: directM3U8s,
          quality_hint: quality,
          country_hint: country,
          source_page: location.href,
        });
      }
      return out;
    });
  });
}

// ---------- Read a /my_list/... page and pull all URLs ----------
async function fetchListBodyViaBrowser(browser, url) {
  return await withPage(browser, url, async (page) => {
    const sel = await page.$('pre, code, textarea');
    if (sel) {
      const txt = await sel.innerText();
      return (txt || '').trim();
    }
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

// ---------- Probe utilities ----------
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

async function fetchHead(url, signal) {
  try {
    const r = await fetch(url, {
      method: 'HEAD',
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/vnd.apple.mpegurl,text/plain,*/*' },
      redirect: 'follow',
      signal,
    });
    return { ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '', finalUrl: r.url };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}
async function fetchRange(url, signal) {
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: {
        'user-agent': 'Mozilla/5.0',
        accept: 'application/vnd.apple.mpegurl,text/plain,*/*',
        Range: 'bytes=0-1023',
      },
      redirect: 'follow',
      signal,
    });
    const txt = await r.text();
    return {
      ok: r.status === 200 || r.status === 206,
      status: r.status,
      ct: r.headers.get('content-type') || '',
      finalUrl: r.url,
      txt,
    };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}
async function fetchText(url, signal) {
  try {
    const r = await fetch(url, {
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/vnd.apple.mpegurl,text/plain,*/*' },
      redirect: 'follow',
      signal,
    });
    const txt = await r.text();
    return { ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '', finalUrl: r.url, txt };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

async function probePlaylist(url) {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), PROBE_TIMEOUT_MS);
  try {
    // HEAD: accept HLS content-type as a pass
    let head = await fetchHead(url, ac.signal);
    if (head.ok && /mpegurl|application\/x-mpegURL/i.test(head.ct)) {
      return { ok: true, url: head.finalUrl, reason: 'head-hls-ct' };
    }

    // Range GET
    let part = await fetchRange(url, ac.signal);
    if (part.ok) {
      if (/#EXTM3U/.test(part.txt)) {
        // If master, choose best variant and verify
        if (/#EXT-X-STREAM-INF/i.test(part.txt)) {
          const best = bestFromMaster(url, part.txt) || url;
          const full = await fetchText(best, ac.signal);
          const ok = full.ok && /#EXTM3U/.test(full.txt);
          return { ok, url: full.finalUrl || best, reason: ok ? 'master-best' : 'master-best-failed' };
        }
        return { ok: true, url, reason: 'media-extm3u' };
      }
      if (/mpegurl|application\/x-mpegURL/i.test(part.ct)) {
        return { ok: true, url: part.finalUrl || url, reason: 'range-hls-ct' };
      }
    }

    // Full GET as last resort
    const full = await fetchText(url, ac.signal);
    if (full.ok && /#EXTM3U/.test(full.txt)) {
      if (/#EXT-X-STREAM-INF/i.test(full.txt)) {
        const best = bestFromMaster(url, full.txt) || url;
        const full2 = await fetchText(best, ac.signal);
        const ok = full2.ok && /#EXTM3U/.test(full2.txt);
        return { ok, url: full2.finalUrl || best, reason: ok ? 'master-best' : 'master-best-failed' };
      }
      return { ok: true, url: full.finalUrl || url, reason: 'media-extm3u' };
    }
    if (full.ok && /mpegurl|application\/x-mpegURL/i.test(full.ct)) {
      return { ok: true, url: full.finalUrl || url, reason: 'get-hls-ct' };
    }

    return { ok: false, url, reason: 'no-extm3u' };
  } catch {
    return { ok: false, url, reason: 'exception' };
  } finally {
    clearTimeout(to);
  }
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

function rankByHint(u, q) {
  const urlScore = /2160|4k|uhd/i.test(u) ? 4 : /1080|fhd/i.test(u) ? 3 : /720|hd/i.test(u) ? 2 : 1;
  const qualScore = /2160|4k|uhd/i.test(q || '') ? 4 : /1080|fhd/i.test(q || '') ? 3 : /720|hd/i.test(q || '') ? 2 : 1;
  return urlScore * 10 + qualScore; // favor 4k>1080>720, tie-break by quality_hint
}

// ---------- Save ----------
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
  const browser = await newBrowser();

  try {
    const pages = await collectPagination(browser);
    console.log('Pages to crawl:', pages.length);
    await fs.writeFile(path.join('out', 'latam', 'pages.json'), JSON.stringify(pages, null, 2), 'utf8');

    const allRows = [];
    for (const url of pages) {
      const rows = await collectRowsFromPage(browser, url);
      allRows.push(...rows);
    }
    await fs.writeFile(path.join('out', 'latam', 'rows_raw.json'), JSON.stringify(allRows, null, 2), 'utf8');

    // Dedup by download_url + collect direct .m3u8 links immediately
    const byDl = new Map();
    for (const r of allRows) {
      const key = r.download_url || `direct:${r.channel_name}:${(r.direct_m3u8s || []).join('|')}`;
      if (!byDl.has(key)) byDl.set(key, r);
    }
    const unique = [...byDl.values()];

    // Expand: for each my_list, fetch/list all m3u8s
    const listBlobs = [];
    for (const row of unique) {
      let urls = [...(row.direct_m3u8s || [])];
      if (row.download_url) {
        const body = await fetchListBodyViaBrowser(browser, row.download_url);
        const found = extractM3U8s(body);
        urls.push(...found);
      }
      urls = [...new Set(urls)];
      listBlobs.push({ ...row, m3u8s: urls });
    }
    await fs.writeFile(path.join('out', 'latam', 'iptvcat_lists.json'), JSON.stringify(listBlobs, null, 2), 'utf8');

    // Probe all candidates
    const probeTargets = [];
    for (const L of listBlobs) {
      for (const u of L.m3u8s) probeTargets.push({ row: L, url: u });
    }
    await fs.writeFile(
      path.join('out', 'latam', 'probe_targets_count.json'),
      JSON.stringify({ targets: probeTargets.length }, null, 2),
      'utf8'
    );

    const probed = await pLimit(PROBE_CONCURRENCY, probeTargets, async (t) => {
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
    await fs.writeFile(path.join('out', 'latam', 'iptvcat_probes.json'), JSON.stringify(probed, null, 2), 'utf8');

    // Pick best working candidate per channel
    const byName = new Map();
    for (const p of probed) {
      if (!p.ok) continue;
      const key = p.channel_name.trim().toLowerCase();
      const candScore = rankByHint(p.candidate_url, p.quality_hint);
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
      source_page_url: b.download_url || b.source_page_url || null,
      source: 'iptvcat',
      working: true,
      checked_at: now,
      extras: { origin: 'iptvcat', probe_reason: b.reason },
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
