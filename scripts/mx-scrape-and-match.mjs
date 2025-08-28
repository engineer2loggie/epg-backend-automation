// Scrape iptv-org (MX), pull m3u8s, probe, parse multiple XMLTV EPGs.
// Prefer M3U tvg-id + optional manual JSON mapping, then brand-weighted fuzzy.
// Write artifacts; upsert streams; optionally ingest programmes.

import { chromium } from 'playwright';
import { createGunzip } from 'node:zlib';
import { Readable } from 'node:stream';
import fs from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createClient } from '@supabase/supabase-js';
import { SaxesParser } from 'saxes';

// ---------- ENV ----------
const SEARCH_URL = process.env.MX_SEARCH_URL || 'https://iptv-org.github.io/?q=live%20country:MX';
const EPG_URLS = (process.env.MX_EPG_URLS || '').trim().split(/\s+/).filter(Boolean);
const M3U_URL = (process.env.M3U_URL || '').trim();
const MAPPING_URL = (process.env.MAPPING_URL || '').trim();

const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const MAX_CHANNELS = Number(process.env.MAX_CHANNELS || '0');
const PER_PAGE_DELAY_MS = Number(process.env.PER_PAGE_DELAY_MS || '150');
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS || '30000');
const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '5000');
const FUZZY_MIN = Number(process.env.FUZZY_MIN || '0.45');
const LOG_UNMATCHED = process.env.LOG_UNMATCHED === '1';

const INGEST_PROGRAMS = (process.env.INGEST_PROGRAMS || '0') === '1';
const PROGRAMS_HOURS_AHEAD = Number(process.env.PROGRAMS_HOURS_AHEAD || '48');

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'mx_channels';
const PROGRAMS_TABLE = process.env.PROGRAMS_TABLE || 'epg_programs';

const EPG_RETRIES = 3;
const EPG_RETRY_DELAY_MS = 2000;

// ---------- NORMALIZATION ----------
function stripAccents(s) { return String(s).normalize('NFD').replace(/\p{Diacritic}+/gu, ''); }
function normalizeNumerals(s) {
  const map = { uno:'1', dos:'2', tres:'3', cuatro:'4', cinco:'5', seis:'6', siete:'7', ocho:'8', nueve:'9', diez:'10', once:'11', doce:'12', trece:'13' };
  return String(s).replace(/\b(uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|trece)\b/gi, m => map[m.toLowerCase()]);
}
function stripLeadingCanal(s) { return String(s).replace(/^\s*canal[\s._-]+/i, ''); }
function dropTimeshift(s) {
  return String(s)
    .replace(/(?:[-+]\s*\d+\s*(?:h|hora|horas)\b)/ig,'')
    .replace(/\b\d+\s*horas?\b/ig,'')
    .replace(/\(\s*\d+\s*horas?\s*\)/ig,'')
    .replace(/\btime\s*shift\b/ig,'')
    .replace(/\s{2,}/g,' ')
    .trim();
}
function stripCountryTail(s) { return String(s).replace(/(\.(mx|us)|\s+\(?mx\)?|\s+m[eé]xico|\s+usa|\s+eeuu)\s*$/i,'').trim(); }
function stripGeoAndCallsigns(s) {
  let x = String(s);
  x = x.replace(/\((?:[^)]*)\)\s*$/,'').trim();           // drop trailing parentheses group from base
  x = x.replace(/\s*-\s*[a-z0-9]{3,5}\b$/i,'').trim();    // drop "- XEW"
  x = x.replace(/\s+(nuevo\s+laredo|monterrey|cdmx|mexico|mx)\b.*$/i,'').trim();
  return x;
}

const STOP = new Set([
  'canal','tv','television','hd','sd','mx','mexico','méxico','hora','horas',
  'us','usa','eeuu','el','la','los','las','de','del','y','en','the','channel'
]);

function tokensOf(s) {
  if (!s) return [];
  let p = stripAccents(normalizeNumerals(String(s).toLowerCase()));
  p = dropTimeshift(p);
  p = stripCountryTail(p);
  p = p.replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, ' ').trim();
  return p.split(/\s+/).filter(t => t && !STOP.has(t));
}
function keyOf(s) { return Array.from(new Set(tokensOf(s))).sort().join(' '); }

function expandNameVariants(s) {
  if (!s) return [];
  const out = new Set();
  const orig = String(s).trim();

  const dotSplit = orig.replace(/[.]/g,' ').replace(/\s+/g,' ').trim();
  const flat = x => x.replace(/[._(),-]+/g,' ').replace(/\s+/g,' ').trim();
  const parenBits = [...orig.matchAll(/\(([^)]+)\)/g)].map(m => m[1]);

  const prelim = [orig, dotSplit, flat(orig)];
  for (const v of prelim) if (v) out.add(v);

  // From parentheses: "Canal Las Estrellas - XEW" → ["Canal Las Estrellas XEW","Canal Las Estrellas","XEW"]
  for (const p of parenBits) {
    const f = flat(p);
    if (f) out.add(f);
    for (const h of p.split(/\s*-\s*/)) out.add(flat(h));
  }

  // Drop "Canal" + geo/callsigns
  out.add(flat(stripGeoAndCallsigns(stripLeadingCanal(orig))));

  // Callsign → brand hints
  const CALL = { xew: 'Las Estrellas', xhlat: 'Azteca 7' };
  for (const key of Object.keys(CALL)) if (orig.toLowerCase().includes(key)) out.add(CALL[key]);

  // Alias harmonization
  const ALIAS = {
    'teleformula': ['telefórmula','tele formula'],
    'milenio television': ['milenio tv','milenio'],
    'azteca 1': ['azteca uno'],
    'azteca 7': ['azteca siete','azteca7'],
    'las estrellas': ['canal de las estrellas','estrellas']
  };
  const addAlias = (v) => {
    const k = v.toLowerCase();
    for (const [canon, alts] of Object.entries(ALIAS)) {
      if (k === canon || alts.some(a => k === a)) {
        out.add(canon); alts.forEach(a => out.add(a));
      }
    }
  };
  [...out].forEach(addAlias);

  return [...out].filter(Boolean);
}

function containsMexicoTag(s) {
  const t = stripAccents(String(s).toLowerCase()).replace(/[^a-z0-9]+/g, ' ').trim();
  const parts = t.split(/\s+/);
  return parts.includes('mexico') || parts.includes('mx') || /\.mx\b/i.test(String(s));
}
function uniqBy(arr, keyFn) {
  const m = new Map();
  for (const x of arr) {
    const k = keyFn(x);
    if (!m.has(k)) m.set(k, x);
  }
  return [...m.values()];
}

// ---------- UTILS ----------
async function retryFetch(url, options = {}, retries = EPG_RETRIES, delayMs = EPG_RETRY_DELAY_MS) {
  for (let i = 0; i < retries; i++) {
    try {
      const r = await fetch(url, options);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r;
    } catch (e) {
      if (i === retries - 1) throw e;
      console.warn(`Fetch failed for ${url} -> retry ${i+1}/${retries}`);
      await delay(delayMs);
    }
  }
}
async function promiseAllWithConcurrency(concurrency, items, asyncFn) {
  const results = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await asyncFn(items[idx], idx);
    }
  }
  await Promise.all(Array(concurrency).fill(0).map(worker));
  return results;
}
function parseXmltvToISO(s) {
  const m = String(s).match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-]\d{4}))?$/);
  if (!m) return null;
  const [, Y,Mo,D,h,mi,se, off] = m;
  const utcMs = Date.UTC(+Y, +Mo-1, +D, +h, +mi, +se);
  let delta = 0;
  if (off) {
    const sign = off.startsWith('-') ? -1 : 1;
    const hh = +off.slice(1,3), mm = +off.slice(3,5);
    delta = sign * (hh*60 + mm) * 60 * 1000;
  }
  const t = utcMs - delta; // → true UTC
  return new Date(t).toISOString();
}

// ---------- OPTIONAL INPUTS (M3U + manual map) ----------
function parseM3U(text) {
  const items = [];
  let cur = null;
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith('#EXTINF')) {
      const attrs = {};
      for (const m of line.matchAll(/\b([a-z0-9_-]+)="([^"]*)"/gi)) attrs[m[1].toLowerCase()] = m[2];
      const comma = line.indexOf(',');
      const title = comma >= 0 ? line.slice(comma + 1).trim() : '';
      cur = { tvg_id: attrs['tvg-id'] || null, tvg_name: attrs['tvg-name'] || title || null, url: null };
    } else if (!line.startsWith('#') && cur) {
      cur.url = line;
      items.push(cur);
      cur = null;
    }
  }
  return items;
}
async function buildM3ULookups() {
  const out = { byUrl: new Map(), byNameKey: new Map() };
  if (!M3U_URL) return out;
  try {
    const txt = await (await retryFetch(M3U_URL)).text();
    const items = parseM3U(txt);
    for (const it of items) if (it.url && it.tvg_id) out.byUrl.set(it.url, it.tvg_id);
    const seen = new Set();
    for (const it of items) {
      const k = keyOf(it.tvg_name || '');
      if (it.tvg_id && k && !seen.has(k)) { out.byNameKey.set(k, it.tvg_id); seen.add(k); }
    }
    console.log(`M3U parsed: ${items.length} entries, url→id:${out.byUrl.size}, name→id:${out.byNameKey.size}`);
  } catch (e) {
    console.warn(`M3U parse skipped: ${e.message}`);
  }
  return out;
}
async function loadManualMap() {
  // JSON array: [{ epg_id, m3u_name, "m3u_tvg-id", custom_channel_name }]
  const out = { byNameKey: new Map(), byTvgId: new Map() };
  if (!MAPPING_URL) return out;
  try {
    const arr = JSON.parse(await (await retryFetch(MAPPING_URL)).text());
    for (const row of arr || []) {
      const epg = row.epg_id || null;
      if (!epg) continue;
      if (row['m3u_tvg-id']) out.byTvgId.set(String(row['m3u_tvg-id']), epg);
      if (row.m3u_name) out.byNameKey.set(keyOf(row.m3u_name), epg);
    }
    console.log(`Manual map loaded: name→${out.byNameKey.size}, tvg→${out.byTvgId.size}`);
  } catch (e) {
    console.warn(`Manual map skipped: ${e.message}`);
  }
  return out;
}

// ---------- SCRAPING ----------
async function collectChannelPages(browser) {
  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT_MS);
  await page.goto(SEARCH_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('a[href*="/channels/"]', { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(800);

  let items = await page.$$eval('a[href*="/channels/"]', as => {
    const out = [];
    for (const a of as) {
      const href = a.getAttribute('href') || '';
      if (!href.includes('/channels/')) continue;
      const url = new URL(href, location.href).href;
      const name = (a.textContent || '').trim();
      out.push({ url, name });
    }
    const m = new Map();
    for (const it of out) if (!m.has(it.url)) m.set(it.url, it);
    return [...m.values()];
  });

  items = items.filter(i => i.name && i.url);
  items = uniqBy(items, x => x.url);
  if (MAX_CHANNELS > 0 && items.length > MAX_CHANNELS) items = items.slice(0, MAX_CHANNELS);
  await page.close();
  return items.map(i => ({ ...i, nameKey: keyOf(i.name) }));
}

async function scrapeChannel(browser, link) {
  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT_MS);
  try {
    await page.goto(link.url, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(400);

    const tab = await page.$('text=Streams');
    if (tab) { await tab.click().catch(() => {}); await page.waitForTimeout(300); }

    let anchors = await page.$$eval('a[href*=".m3u8"]', els =>
      els.map(e => ({ url: e.href, text: (e.textContent || '').trim() }))
    );
    if (!anchors.length) {
      const html = await page.content();
      const rx = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
      const set = new Set(); let m; while ((m = rx.exec(html))) set.add(m[0]);
      anchors = [...set].map(u => ({ url: u, text: '' }));
    }

    anchors = uniqBy(anchors.filter(a => /^https?:\/\//i.test(a.url)), a => a.url);
    return anchors.map(a => ({
      url: a.url,
      quality: (a.text.match(/\b(1080p|720p|480p|360p|HD|SD)\b/i) || [])[0] || null
    }));
  } catch (e) {
    console.error(`Error scraping ${link.url}: ${e.message}`);
    return [];
  } finally {
    await page.close();
  }
}

async function scrapeAll(browser, links) {
  const res = await promiseAllWithConcurrency(10, links, async (lnk) => {
    const streams = await scrapeChannel(browser, lnk);
    await delay(PER_PAGE_DELAY_MS);
    return streams.length ? { channelName: lnk.name, channelNameKey: lnk.nameKey, streams } : null;
  });
  return res.filter(Boolean);
}

async function probeM3U8(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), PROBE_TIMEOUT_MS);
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: { 'user-agent': 'Mozilla/5.0', 'accept': 'application/vnd.apple.mpegurl,text/plain,*/*' },
      signal: ac.signal
    });
    if (!r.ok) return false;
    const txt = await r.text();
    return txt.includes('#EXTM3U');
  } catch { return false; }
  finally { clearTimeout(t); }
}

// ---------- EPG PARSING ----------
function isMexicoEntry(id, names) {
  return /\.mx$/i.test(id || '') || (names || []).some(n => /\b(m[eé]xico|mx)\b/i.test(String(n)));
}

async function parseOneEpg(url, agg, keepAll) {
  console.log(`Downloading EPG (stream)… ${url}`);
  const res = await retryFetch(url);
  const gunzip = createGunzip();
  const src = Readable.fromWeb(res.body);
  const decoder = new TextDecoder('utf-8');
  const parser = new SaxesParser({ xmlns: false });

  const MAX_NAME_CHARS = 1024, MAX_NAMES_PER_CH = 24, MAX_VARIANTS = 64;

  let cur = null, inDisp = false, dispChunks = [], dispLen = 0, dispTruncated = false;

  // Programme capture (optional)
  let prog = null, buf = '', inTag = null;
  const keepWindowTo = new Date(Date.now() + PROGRAMS_HOURS_AHEAD*3600*1000);

  parser.on('error', (e) => { throw e; });

  parser.on('opentag', (tag) => {
    const nm = String(tag.name).toLowerCase();
    if (nm === 'channel') {
      cur = { id: tag.attributes?.id ? String(tag.attributes.id) : '', namesRaw: [] };
    } else if (nm === 'display-name' && cur) {
      inDisp = true; dispChunks = []; dispLen = 0; dispTruncated = false;
    } else if (nm === 'programme') {
      if (!INGEST_PROGRAMS) return;
      const cid = String(tag.attributes?.channel || '');
      const st = parseXmltvToISO(tag.attributes?.start || '');
      const en = parseXmltvToISO(tag.attributes?.stop || '');
      const stDate = st ? new Date(st) : null;
      if (cid && st && en && stDate && stDate <= keepWindowTo) {
        prog = {
          channel_id: cid, start_ts: st, stop_ts: en,
          title: null, sub_title: null, summary: null,
          categories: [], program_url: null, episode_num_xmltv: null,
          icon_url: null, rating: null, star_rating: null,
          season: null, episode: null, language: null, orig_language: null,
          credits: null, premiere: false, previously_shown: false,
          extras: null, ingested_at: new Date().toISOString()
        };
      } else {
        prog = null;
      }
    } else if (prog && ['title','sub-title','desc','category','url','episode-num','language','orig-language','star-rating','premiere','previously-shown','icon','credits','rating'].includes(nm)) {
      inTag = nm; buf = '';
      if (nm === 'icon') {
        const u = tag.attributes?.src ? String(tag.attributes.src) : null;
        if (u) prog.icon_url = u;
      }
    }
  });

  parser.on('text', (t) => {
    if (inDisp && cur && t && !dispTruncated) {
      let chunk = String(t);
      if (chunk.length > MAX_NAME_CHARS) chunk = chunk.slice(0, MAX_NAME_CHARS);
      const remain = MAX_NAME_CHARS - dispLen;
      if (remain <= 0) { dispTruncated = true; return; }
      if (chunk.length > remain) { chunk = chunk.slice(0, remain); dispTruncated = true; }
      if (chunk) { dispChunks.push(chunk); dispLen += chunk.length; }
    }
    if (prog && inTag && t) buf += t;
  });

  parser.on('closetag', async (nameRaw) => {
    const nm = String(nameRaw).toLowerCase();
    if (nm === 'display-name' && cur) {
      if (cur.namesRaw.length < MAX_NAMES_PER_CH) {
        const txt = dispChunks.length ? dispChunks.join('') : '';
        const clean = txt.trim();
        if (clean) cur.namesRaw.push(clean);
      }
      inDisp = false; dispChunks = []; dispLen = 0; dispTruncated = false;
    } else if (nm === 'channel' && cur) {
      const id = cur.id || '';
      const keep = keepAll ? true : isMexicoEntry(id, cur.namesRaw);
      if (keep) {
        const names = new Set();
        for (const n of cur.namesRaw) for (const v of expandNameVariants(n)) if (v) names.add(v);
        for (const v of expandNameVariants(id)) if (v) names.add(v);
        const limited = []; for (const v of names) { limited.push(v); if (limited.length >= MAX_VARIANTS) break; }
        let entry = agg.channels.get(id);
        if (!entry) { entry = { id, names: [], tokenSet: new Set() }; agg.channels.set(id, entry); }
        entry.names = Array.from(new Set(entry.names.concat(limited))).slice(0, MAX_VARIANTS);
        entry.tokenSet = new Set(); for (const nm2 of entry.names) for (const tok of tokensOf(nm2)) entry.tokenSet.add(tok);
        for (const n of entry.names) {
          const k = keyOf(n);
          if (k && !agg.nameMap.has(k)) agg.nameMap.set(k, entry);
        }
      }
      cur = null;
    } else if (prog && nm === inTag) {
      const v = (buf || '').trim();
      if (v) {
        if (inTag === 'title') prog.title = v;
        else if (inTag === 'sub-title') prog.sub_title = v;
        else if (inTag === 'desc') prog.summary = v;
        else if (inTag === 'category') prog.categories.push(v);
        else if (inTag === 'url') prog.program_url = v;
        else if (inTag === 'episode-num') prog.episode_num_xmltv = v;
        else if (inTag === 'language') prog.language = v;
        else if (inTag === 'orig-language') prog.orig_language = v;
        else if (inTag === 'star-rating') prog.star_rating = v;
        else if (inTag === 'premiere') prog.premiere = true;
        else if (inTag === 'previously-shown') prog.previously_shown = true;
        else if (inTag === 'rating') prog.rating = { text: v };
      }
      inTag = null; buf = '';
    } else if (nm === 'programme' && prog) {
      agg.programs.push(prog);
      prog = null;
    }
  });

  await new Promise((resolve, reject) => {
    src.on('error', reject);
    gunzip.on('error', reject);
    gunzip.on('data', (chunk) => {
      const text = decoder.decode(chunk, { stream: true });
      if (text) parser.write(text);
    });
    gunzip.on('end', () => {
      parser.write(decoder.decode(new Uint8Array(), { stream: false }));
      parser.close();
      resolve();
    });
    src.pipe(gunzip);
  });
}

async function parseAllEpg(urls) {
  const agg = { channels: new Map(), nameMap: new Map(), programs: [] };
  let ok = 0;
  for (const url of urls) {
    try {
      await parseOneEpg(url, agg, /_MX/i.test(url)); // keep-all if filename hints MX
      ok++;
    } catch (e) {
      console.warn(`EPG parse failed ${url}: ${e.message}`);
    }
  }
  console.log(`EPG files parsed OK: ${ok}/${urls.length}`);
  const kept = new Set([...agg.nameMap.values()]);
  console.log(`EPG entries kept (Mexico-related): ${kept.size}`);
  return { nameMap: agg.nameMap, entries: [...kept], programs: agg.programs };
}

// ---------- MATCH (brand-weighted fuzzy) ----------
const BRAND = new Set(['azteca','adn','milenio','fox','sports','premium','estrellas','teleformula','maria','vision','las']);
function scorePair(aTokens, bTokens) {
  const A = new Set(aTokens), B = new Set(bTokens);
  let inter = 0; for (const t of A) if (B.has(t)) inter++;
  const jacc = inter / (A.size + B.size - inter || 1);
  let brandHits = 0; for (const t of A) if (BRAND.has(t) && B.has(t)) brandHits++;
  const bonus = Math.min(brandHits, 3) * 0.05;  // up to +0.15
  return jacc + bonus;
}
function findMatchByName(channelName, nameKey, nameMap, entries) {
  const exact = nameMap.get(nameKey);
  if (exact) return { entry: exact, score: 1, method: 'exact' };

  const sTokArr = tokensOf(channelName);
  let best = null, bestScore = 0;

  for (const e of entries) for (const nm of e.names) {
    const sc = scorePair(sTokArr, tokensOf(nm));
    if (sc > bestScore) { bestScore = sc; best = e; }
  }
  if (best && bestScore >= (FUZZY_MIN - 0.05)) return { entry: best, score: bestScore, method: 'brand-fuzzy' };
  return { entry: null, score: bestScore, method: 'none' };
}

// ---------- DB ----------
function sbClient() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, { auth: { persistSession: false }, db: { schema: SUPABASE_SCHEMA } });
}
async function saveStreams(rows) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('Supabase env missing; skipped DB upload.');
    return;
  }
  if (!rows.length) { console.log('No stream rows to upload.'); return; }

  const tryUpsert = async (fields) => {
    const sb = sbClient();
    const payload = rows.map(r => {
      const o = {
        stream_url: r.stream_url,
        channel_guess: r.channel_guess,
        epg_channel_id: r.epg_channel_id,
        epg_display_name: r.epg_display_name,
        working: r.working,
        checked_at: r.checked_at
      };
      if (fields.tvg_id && r.tvg_id != null) o.tvg_id = r.tvg_id;
      if (fields.channel_name && r.channel_name != null) o.channel_name = r.channel_name;
      return o;
    });
    return await sb.from(SUPABASE_TABLE).upsert(payload, { onConflict: 'stream_url', ignoreDuplicates: false });
  };

  // Try with tvg_id + channel_name; then fallback if columns don’t exist
  let fields = { tvg_id: true, channel_name: true };
  let { error } = await tryUpsert(fields);
  if (error && /column .*tvg_id/i.test(error.message || '')) { fields.tvg_id = false; ({ error } = await tryUpsert(fields)); }
  if (error && /column .*channel_name/i.test(error.message || '')) { fields.channel_name = false; ({ error } = await tryUpsert(fields)); }

  if (error) console.warn(`Streams upsert failed: ${error.message} (${error.code ?? 'no-code'})`);
  else console.log(`Streams DB write OK: ${rows.length} rows`);
}

async function savePrograms(programs) {
  if (!INGEST_PROGRAMS || !programs.length) return;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) { console.log('Supabase env missing; skipped programs upload.'); return; }

  const sb = sbClient();
  const BATCH = 500;
  for (let i = 0; i < programs.length; i += BATCH) {
    const slice = programs.slice(i, i + BATCH);
    // Prefer upsert on (channel_id, start_ts); fallback to insert if constraint missing
    let { error } = await sb
      .from(PROGRAMS_TABLE)
      .upsert(slice, { onConflict: 'channel_id, start_ts' });
    if (error && /no unique|no exclusion constraint/i.test(error.message || '')) {
      ({ error } = await sb.from(PROGRAMS_TABLE).insert(slice));
    }
    if (error) { console.warn(`Programs batch failed: ${error.message}`); break; }
  }
  console.log(`Program ingest attempted for ${programs.length} rows`);
}

// ---------- MAIN ----------
async function ensureDir(p) { await fs.mkdir(p, { recursive: true }); }

async function main() {
  if (!EPG_URLS.length) throw new Error('No EPG URLs provided in MX_EPG_URLS');

  await ensureDir('out/mx');
  const browser = await chromium.launch({ headless: HEADLESS });

  const m3u = await buildM3ULookups();
  const manual = await loadManualMap();

  try {
    console.log(`Scraping: ${SEARCH_URL}`);
    const links = await collectChannelPages(browser);
    console.log(`Found ${links.length} channel pages.`);
    const scraped = await scrapeAll(browser, links);
    console.log(`Channels with at least one .m3u8 (before probe): ${scraped.length}`);

    // Probe streams (parallel)
    const allUrls = [...new Set(scraped.flatMap(ch => ch.streams.map(s => s.url)))];
    console.log(`Probing ${allUrls.length} unique streams in parallel...`);
    const probe = await promiseAllWithConcurrency(50, allUrls, async (u) => ({ u, ok: await probeM3U8(u) }));
    const workingSet = new Set(probe.filter(x => x.ok).map(x => x.u));
    const filtered = scraped.map(ch => ({ ...ch, streams: ch.streams.filter(s => workingSet.has(s.url)) }))
                            .filter(ch => ch.streams.length > 0);
    console.log(`Channels with at least one WORKING .m3u8: ${filtered.length}`);

    const { nameMap, entries, programs } = await parseAllEpg(EPG_URLS);

    // Match + records
    const records = [];
    const matchedOnly = [];
    const unmatched = [];

    for (const r of filtered) {
      // Prefer exact IDs from M3U or manual mapping
      const tvgIdFromName = m3u.byNameKey.get(r.channelNameKey) || null;
      let manualEpg = manual.byNameKey.get(r.channelNameKey) || (tvgIdFromName ? manual.byTvgId.get(tvgIdFromName) : null);

      // Name-based best match
      const m = findMatchByName(r.channelName, r.channelNameKey, nameMap, entries);
      const entry = manualEpg ? entries.find(e => e.id === manualEpg) || m.entry : m.entry;
      const method = manualEpg && entry ? 'manual-map' : m.method;
      const bestId = (entry && entry.id) || tvgIdFromName || null;

      for (const s of r.streams) {
        const urlTvg = m3u.byUrl.get(s.url) || null;
        const tvg_id = urlTvg || tvgIdFromName || bestId;

        const rec = {
          stream_url: s.url,
          channel_guess: r.channelName,
          tvg_id,
          epg_channel_id: (entry && entry.id) || tvg_id || null,
          epg_display_name: entry ? (entry.names[0] || null) : null,
          working: true,
          checked_at: new Date().toISOString(),
          channel_name: entry ? (entry.names[0] || r.channelName) : r.channelName
        };
        records.push(rec);
        if (entry) matchedOnly.push({ ...rec, _match_method: method });
        else unmatched.push({ ...rec, _match_method: 'none' });
      }
    }

    console.log(`Matched with EPG: ${matchedOnly.length} stream rows (across ${filtered.length} channels).`);

    // Suggestions for manual mapping (top-3)
    const suggestions = [];
    for (const rec of unmatched) {
      const sTok = tokensOf(rec.channel_guess);
      const top = entries.map(e => ({
        epg_id: e.id,
        epg_name: e.names[0] || null,
        score: scorePair(sTok, tokensOf(e.names[0] || e.id))
      })).sort((a,b)=>b.score-a.score).slice(0,3);
      suggestions.push({ channel_guess: rec.channel_guess, stream_url: rec.stream_url, candidates: top });
    }

    // Artifacts
    await fs.writeFile(path.join('out','mx','records.json'), JSON.stringify(records,null,2), 'utf8');
    await fs.writeFile(path.join('out','mx','matches.json'), JSON.stringify(matchedOnly,null,2), 'utf8');
    if (LOG_UNMATCHED) await fs.writeFile(path.join('out','mx','unmatched.json'), JSON.stringify(unmatched,null,2), 'utf8');
    await fs.writeFile(path.join('out','mx','map_suggestions.json'), JSON.stringify(suggestions,null,2), 'utf8');

    // DB writes
    await saveStreams(records);

    // Program handling
    if (INGEST_PROGRAMS) {
      // sample artifact only; full set goes to DB
      await fs.writeFile(path.join('out','mx','epg_programs_sample.json'),
        JSON.stringify(programs.slice(0,300), null, 2), 'utf8');
      await savePrograms(programs);
    }
  } finally {
    await browser.close();
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
