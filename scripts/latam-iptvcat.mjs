// scripts/latam-iptvcat.mjs
// fetch-only iptvcat scraper:
// - Option A: crawl a listing page (and optional pagination) to collect /my_list/*.m3u8 + direct .m3u8
// - Option B: if IPTVCAT_MYLIST_URL is set, fetch just that list and parse
// - Parse M3U bodies (#EXTINF) into channel items; also accept raw .m3u8 lines
// - Probe candidates (no browser), pick best per channel, upsert to streams_latam

import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { DateTime } from 'luxon';
import * as cheerio from 'cheerio';

const START_URL = process.env.IPTVCAT_START_URL || '';
const MAX_PAGES = Number(process.env.IPTVCAT_MAX_PAGES || '0');
const MYLIST_URL = (process.env.IPTVCAT_MYLIST_URL || '').trim();

const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '10000');
const PROBE_CONCURRENCY = Number(process.env.PROBE_CONCURRENCY || '8');

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const STREAMS_TABLE = process.env.STREAMS_TABLE || 'streams_latam';

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36';

function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

async function fetchHtml(url) {
  const r = await fetch(url, {
    headers: { 'user-agent': UA, accept: 'text/html,*/*' },
    redirect: 'follow',
  });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return await r.text();
}

async function fetchText(url, referer) {
  const r = await fetch(url, {
    headers: {
      'user-agent': UA,
      accept: 'application/x-mpegURL,text/plain,*/*',
      ...(referer ? { referer } : {}),
    },
    redirect: 'follow',
  });
  if (!r.ok) return '';
  return await r.text();
}

// ---- Listing crawl (no headless) ----
async function collectPages(startUrl) {
  if (!startUrl) return [];
  try {
    const html = await fetchHtml(startUrl);
    const $ = cheerio.load(html);
    const set = new Set([new URL(startUrl).href]);
    // discover simple pagination links that look like ".../latin_america__7/2"
    $('a[href]').each((_, a) => {
      const href = $(a).attr('href');
      if (!href) return;
      try {
        const u = new URL(href, startUrl).href;
        if (/latin_america__7(\/\d+\/?)?$/i.test(u)) set.add(u);
      } catch {}
    });
    const urls = [...set];
    return MAX_PAGES > 0 ? urls.slice(0, MAX_PAGES + 1) : urls;
  } catch {
    return [startUrl];
  }
}

function textOf($el) {
  return $el.text().replace(/\s+/g, ' ').trim();
}

function parseListingRows(html, baseUrl) {
  const $ = cheerio.load(html);
  const out = [];

  // Strategy: each row may contain channel name, a "/my_list/....m3u8" link, and sometimes direct .m3u8 anchors.
  // We try a few generic patterns so we don't depend on a fragile selector.

  $('tr').each((_, tr) => {
    const $tr = $(tr);
    const tds = $tr.find('td');

    // guess channel name from first non-empty cell
    let name = '';
    for (let i = 0; i < tds.length; i++) {
      const v = textOf($(tds[i]));
      if (v) {
        name = v;
        break;
      }
    }

    // country hint from last cell (best-effort)
    const country = tds.length ? textOf($(tds[tds.length - 1])) : '';

    // download link (my_list)
    let download_url = null;
    const dl = $tr.find('a[href*="/my_list/"]').first();
    if (dl.length) {
      try {
        download_url = new URL(dl.attr('href'), baseUrl).href;
      } catch {}
    }

    // direct m3u8s in the row
    const direct = [];
    $tr.find('a[href$=".m3u8"]').each((_, a) => {
      const href = $(a).attr('href');
      if (!href) return;
      try {
        direct.push(new URL(href, baseUrl).href);
      } catch {}
    });

    if (!name && !download_url && direct.length === 0) return;

    out.push({
      channel_name: name || null,
      download_url,
      direct_m3u8s: [...new Set(direct)],
      country_hint: country || null,
      source_page: baseUrl,
    });
  });

  // Also scan any loose "my_list" anchors not in table rows
  $('a[href*="/my_list/"]').each((_, a) => {
    const href = $(a).attr('href');
    if (!href) return;
    const name = textOf($(a).closest('tr').find('td').first());
    try {
      const url = new URL(href, baseUrl).href;
      out.push({
        channel_name: name || null,
        download_url: url,
        direct_m3u8s: [],
        country_hint: null,
        source_page: baseUrl,
      });
    } catch {}
  });

  return out;
}

function parseM3UToItems(text) {
  // Support both full M3U with #EXTINF and raw URLs-only
  const items = [];
  let current = null;
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith('#EXTINF')) {
      const attrs = {};
      for (const m of line.matchAll(/\b([a-zA-Z0-9_-]+)="([^"]*)"/g)) {
        attrs[m[1].toLowerCase()] = m[2];
      }
      const comma = line.indexOf(',');
      const title = comma >= 0 ? line.slice(comma + 1).trim() : '';
      current = {
        tvg_id: attrs['tvg-id'] || null,
        tvg_name: attrs['tvg-name'] || title || null,
        url: null,
      };
    } else if (!line.startsWith('#')) {
      if (current) {
        current.url = line;
        items.push(current);
        current = null;
      } else if (/^https?:\/\/.+\.m3u8/i.test(line)) {
        items.push({ tvg_id: null, tvg_name: null, url: line });
      }
    }
  }
  return items;
}

function extractM3U8sFromText(text) {
  const out = [];
  const re = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
  let m;
  while ((m = re.exec(text))) out.push(m[0]);
  return [...new Set(out)];
}

async function getListCandidates(row) {
  // combine direct m3u8s + any parsed from the download_url body
  let urls = [...(row.direct_m3u8s || [])];
  if (row.download_url) {
    const body = await fetchText(row.download_url, row.source_page);
    if (body) {
      const items = parseM3UToItems(body);
      if (items.length) {
        urls.push(...items.map((i) => i.url).filter(Boolean));
      } else {
        urls.push(...extractM3U8sFromText(body));
      }
    }
  }
  return [...new Set(urls)];
}

// Probe a candidate URL
async function probePlaylist(url) {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), PROBE_TIMEOUT_MS);
  try {
    const r = await fetch(url, {
      headers: {
        'user-agent': UA,
        accept: 'application/vnd.apple.mpegurl,text/plain,*/*',
      },
      redirect: 'follow',
      signal: ac.signal,
    });
    const text = await r.text();
    const ok = r.ok && /#EXTM3U/.test(text);
    return { ok, url: r.url || url, body: ok ? text : '' };
  } catch (e) {
    return { ok: false, url, error: String(e) };
  } finally {
    clearTimeout(to);
  }
}

function bestFromMaster(baseUrl, text) {
  const lines = (text || '').split(/\r?\n/);
  const variants = [];
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
        variants.push({ bw, url: new URL(next, baseUrl).href });
      } catch {
        variants.push({ bw, url: next });
      }
    }
  }
  variants.sort((a, b) => b.bw - a.bw);
  return variants[0]?.url || null;
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

function rankByHeuristics(url) {
  // prefer 2160/4k > 1080 > 720 > 480
  if (/2160|4k|uhd/i.test(url)) return 40;
  if (/1080|fhd/i.test(url)) return 30;
  if (/720|hd/i.test(url)) return 20;
  return 10;
}

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

  // Short-circuit: fetch a single my_list URL if provided
  if (MYLIST_URL) {
    const body = await fetchText(MYLIST_URL, 'https://iptvcat.com/');
    await fs.writeFile(path.join('out', 'latam', 'mylist_body.m3u8'), body, 'utf8');
    const items = parseM3UToItems(body);
    const rawUrls = items.length ? items.map((i) => i.url).filter(Boolean) : extractM3U8sFromText(body);
    const targets = [...new Set(rawUrls)].map((u) => ({
      channel_name: null,
      country_hint: null,
      candidate_url: u,
    }));
    console.log(`Single my_list: candidates = ${targets.length}`);

    const probed = await pLimit(PROBE_CONCURRENCY, targets, async (t) => {
      const pr = await probePlaylist(t.candidate_url);
      let finalUrl = pr.url;
      if (pr.ok && /#EXT-X-STREAM-INF/i.test(pr.body)) {
        const best = bestFromMaster(finalUrl, pr.body);
        if (best) finalUrl = best;
      }
      return { ok: pr.ok, channel_name: t.channel_name, country_hint: t.country_hint, url: finalUrl };
    });

    const now = DateTime.utc().toISO();
    const ups = probed
      .filter((p) => p.ok)
      .map((p) => ({
        stream_url: p.url,
        channel_name: p.channel_name, // may be null
        country_hint: p.country_hint,
        source_page_url: MYLIST_URL,
        source: 'iptvcat',
        working: true,
        checked_at: now,
        extras: { origin: 'iptvcat', mode: 'single-list' },
      }));
    await fs.writeFile(path.join('out', 'latam', 'streams_latam.json'), JSON.stringify(ups, null, 2), 'utf8');
    await saveStreams(ups);
    console.log(`iptvcat single-list done. Rows kept: ${ups.length}`);
    return;
  }

  // Otherwise: crawl a listing page (and optional pagination)
  const pages = await collectPages(START_URL);
  await fs.writeFile(path.join('out', 'latam', 'pages.json'), JSON.stringify(pages, null, 2), 'utf8');
  console.log(`Pages to crawl: ${pages.length}`);

  const allRows = [];
  for (const url of pages) {
    const html = await fetchHtml(url);
    const rows = parseListingRows(html, url);
    allRows.push(...rows);
  }
  await fs.writeFile(path.join('out', 'latam', 'rows_raw.json'), JSON.stringify(allRows, null, 2), 'utf8');

  // De-dup per listing row
  const byKey = new Map();
  for (const r of allRows) {
    const key = `${r.channel_name || ''}|${r.download_url || ''}|${(r.direct_m3u8s || []).join('|')}`;
    if (!byKey.has(key)) byKey.set(key, r);
  }
  const uniqueRows = [...byKey.values()];

  // Expand to candidate m3u8s
  const expanded = [];
  for (const row of uniqueRows) {
    const urls = await getListCandidates(row);
    expanded.push({ ...row, m3u8s: urls });
  }
  await fs.writeFile(path.join('out', 'latam', 'lists_expanded.json'), JSON.stringify(expanded, null, 2), 'utf8');

  const candidates = [];
  for (const E of expanded)
    for (const u of E.m3u8s) {
      candidates.push({ name: E.channel_name, country: E.country_hint, page: E.source_page, url: u });
    }
  await fs.writeFile(
    path.join('out', 'latam', 'probe_targets.json'),
    JSON.stringify({ count: candidates.length }, null, 2),
    'utf8'
  );

  // Probe concurrently
  const probed = await pLimit(PROBE_CONCURRENCY, candidates, async (t) => {
    const pr = await probePlaylist(t.url);
    let finalUrl = pr.url;
    let masterBonus = 0;
    if (pr.ok && /#EXT-X-STREAM-INF/i.test(pr.body)) {
      const best = bestFromMaster(finalUrl, pr.body);
      if (best) finalUrl = best;
      masterBonus = 50;
    }
    const score = (pr.ok ? 100 : 0) + masterBonus + rankByHeuristics(finalUrl);
    return { ok: pr.ok, name: t.name, country: t.country, page: t.page, url: finalUrl, score };
  });
  await fs.writeFile(path.join('out', 'latam', 'probes.json'), JSON.stringify(probed, null, 2), 'utf8');

  // Pick best per channel name (fallback key if name is blank: the page URL)
  const bestByKey = new Map();
  for (const p of probed) {
    if (!p.ok) continue;
    const key = (p.name && p.name.trim().toLowerCase()) || `page:${p.page}`;
    const cur = bestByKey.get(key);
    if (!cur || p.score > cur.score) bestByKey.set(key, p);
  }
  const best = [...bestByKey.values()];
  await fs.writeFile(path.join('out', 'latam', 'best.json'), JSON.stringify(best, null, 2), 'utf8');

  const now = DateTime.utc().toISO();
  const upserts = best.map((b) => ({
    stream_url: b.url,
    channel_name: b.name || null,
    country_hint: b.country || null,
    source_page_url: b.page || null,
    source: 'iptvcat',
    working: true,
    checked_at: now,
    extras: { origin: 'iptvcat', rank: b.score },
  }));
  await fs.writeFile(path.join('out', 'latam', 'streams_latam.json'), JSON.stringify(upserts, null, 2), 'utf8');

  await saveStreams(upserts);
  console.log(`iptvcat ingestion done. Channels kept: ${upserts.length}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});