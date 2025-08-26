// Scrape iptv-org for MX channels, pull .m3u8s, probe them,
// stream-parse THREE smaller EPG XML.GZ files safely,
// match channels (exact/anchor/subset/fuzzy), write artifacts,
// and insert into Supabase public.mx_channels:
//   stream_url, channel_guess, epg_channel_id, epg_display_name, working, checked_at

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
const EPG_SOURCES =
  (process.env.EPG_SOURCES &&
    process.env.EPG_SOURCES.split(',').map(s => s.trim()).filter(Boolean)) ||
  [
    'https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS2.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz',
  ];

const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const MAX_CHANNELS = Number(process.env.MAX_CHANNELS || '0'); // 0 = unlimited
const PER_PAGE_DELAY_MS = Number(process.env.PER_PAGE_DELAY_MS || '150');
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS || '30000');
const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '5000');

const FUZZY_MIN = Number(process.env.FUZZY_MIN || '0.45');
const LOG_UNMATCHED = process.env.LOG_UNMATCHED === '1';
const EPG_REQUIRE_PROGS = (process.env.EPG_REQUIRE_PROGS || '0') === '1'; // keep channels only if they have any <programme>

// Supabase
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'mx_channels';

// ---------- NORMALIZATION ----------
function stripAccents(s) { return String(s).normalize('NFD').replace(/\p{Diacritic}+/gu, ''); }
function normalizeNumerals(s) {
  const map = { uno:'1', dos:'2', tres:'3', cuatro:'4', cinco:'5', seis:'6', siete:'7', ocho:'8', nueve:'9', diez:'10', once:'11', doce:'12', trece:'13' };
  return String(s).replace(/\b(uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|trece)\b/gi, m => map[m.toLowerCase()]);
}
function dropTimeshift(s) {
  return String(s)
    .replace(/(?:[-+]\s*\d+\s*(?:h|hora|horas)\b)/ig,'')
    .replace(/\b\d+\s*horas?\b/ig,'')
    .replace(/\(\s*\d+\s*horas?\s*\)/ig,'')
    .replace(/\btime\s*shift\b/ig,'')
    .replace(/\s{2,}/g,' ')
    .trim();
}
function stripLeadingCanal(s) { return String(s).replace(/^\s*canal[\s._-]+/i, ''); }
function stripCountryTag(s) {
  return String(s).replace(/(\.(mx|us)|\s+\(?mx\)?|\s+m[eé]xico|\s+usa|\s+eeuu)\s*$/i,'').trim();
}
const STOP = new Set(['canal','tv','television','hd','sd','mx','mexico','méxico','hora','horas','us','usa','eeuu']);
function tokensOf(s) {
  if (!s) return [];
  let plain = stripAccents(normalizeNumerals(String(s).toLowerCase()));
  plain = dropTimeshift(plain);
  plain = stripCountryTag(plain);
  plain = plain.replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, ' ').trim();
  return plain.split(/\s+/).filter(t => t && !STOP.has(t));
}
function keyOf(s) { return Array.from(new Set(tokensOf(s))).sort().join(' '); }

function expandNameVariants(s) {
  if (!s) return [];
  const out = new Set();
  const orig = String(s).trim();
  const noCanal = stripLeadingCanal(orig);
  const flat = x => x.replace(/[._]+/g, ' ').replace(/\s+/g, ' ').trim();
  const noTS = dropTimeshift(noCanal);
  const noCountry = stripCountryTag(noTS);
  [orig, noCanal, noTS, noCountry, flat(orig), flat(noCanal), flat(noTS), flat(noCountry)]
    .forEach(v => { if (v) out.add(v); });
  return [...out];
}

function uniqBy(arr, keyFn) {
  const m = new Map();
  for (const x of arr) {
    const k = keyFn(x);
    if (!m.has(k)) m.set(k, x);
  }
  return [...m.values()];
}

// ---------- SCRAPING ----------
async function collectChannelPages(browser) {
  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT_MS);
  await page.goto(SEARCH_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('a[href*="/channels/"]', { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(1000);

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
    await page.waitForTimeout(500);

    const tab = await page.$('text=Streams');
    if (tab) { await tab.click().catch(() => {}); await page.waitForTimeout(400); }

    let anchors = await page.$$eval('a[href*=".m3u8"]', els =>
      els.map(e => ({ url: e.href, text: (e.textContent || '').trim() }))
    );

    if (!anchors.length) {
      const html = await page.content();
      const rx = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
      const set = new Set();
      let m; while ((m = rx.exec(html))) set.add(m[0]);
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
  const out = [];
  for (const lnk of links) {
    const streams = await scrapeChannel(browser, lnk);
    if (streams.length) {
      out.push({
        channelName: lnk.name,
        channelNameKey: lnk.nameKey,
        streams
      });
    }
    await delay(PER_PAGE_DELAY_MS);
  }
  return out;
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

// ---------- EPG STREAM PARSE (three files) ----------
function mexicoish(id, names) {
  if (/\.mx\b/i.test(id)) return true;
  return names.some(n => /méxico|mexico|\bmx\b/i.test(String(n)));
}

async function parseManyEpgStreams(epgUrls, scrapedTokenUniverse) {
  const globalIdTo = new Map();   // id -> entry
  const nameMap = new Map();      // normalized name key -> entry

  for (const url of epgUrls) {
    console.log(`Downloading EPG (stream)… ${url}`);
    const res = await fetch(url);
    if (!res.ok || !res.body) { console.warn(`Fetch failed ${res.status} ${url}`); continue; }

    const gunzip = createGunzip();
    const src = Readable.fromWeb(res.body);
    const decoder = new TextDecoder('utf-8');
    const parser = new SaxesParser({ xmlns: false });

    // caps
    const MAX_NAME_CHARS = 512;
    const MAX_NAMES_PER_CH = 24;
    const MAX_VARIANTS = 64;

    const programmesSeen = new Set();

    // channel state
    let cur = null; // { id, namesRaw:[] }
    let inDisp = false;
    let dispChunks = [];
    let dispLen = 0;
    let dispTruncated = false;

    parser.on('error', (e) => { throw e; });

    parser.on('opentag', (tag) => {
      const nm = String(tag.name).toLowerCase();
      if (nm === 'channel') {
        cur = { id: tag.attributes?.id ? String(tag.attributes.id) : '', namesRaw: [] };
      } else if (nm === 'display-name' && cur) {
        inDisp = true; dispChunks = []; dispLen = 0; dispTruncated = false;
      } else if (nm === 'programme') {
        const cid = tag.attributes?.channel;
        if (cid) programmesSeen.add(String(cid));
      }
    });

    parser.on('text', (t) => {
      // bounded <display-name> accumulation
      if (!inDisp || !cur || !t || dispTruncated) return;
      let chunk = String(t);
      if (chunk.length > MAX_NAME_CHARS) chunk = chunk.slice(0, MAX_NAME_CHARS);
      const remain = MAX_NAME_CHARS - dispLen;
      if (remain <= 0) { dispTruncated = true; return; }
      if (chunk.length > remain) { chunk = chunk.slice(0, remain); dispTruncated = true; }
      if (chunk) { dispChunks.push(chunk); dispLen += chunk.length; }
    });

    parser.on('closetag', (nameRaw) => {
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
        // keep only Mexico-related channels, and intersect tokens with scraped universe to shrink set
        const mex = mexicoish(id, cur.namesRaw);
        if (mex) {
          // Build variants & tokens
          const names = new Set();
          for (const n of cur.namesRaw) for (const v of expandNameVariants(n)) if (v) names.add(v);
          for (const v of expandNameVariants(id)) if (v) names.add(v);

          // check token intersection
          let intersects = false;
          const tokenSet = new Set();
          for (const nm2 of names) for (const tok of tokensOf(nm2)) {
            tokenSet.add(tok);
            if (!intersects && scrapedTokenUniverse.has(tok)) intersects = true;
          }

          if (intersects) {
            const existing = globalIdTo.get(id);
            if (!existing) {
              const entry = { id, names: [], tokenSet, hasProgs: false };
              // clip variants
              for (const v of names) { entry.names.push(v); if (entry.names.length >= MAX_VARIANTS) break; }
              globalIdTo.set(id, entry);
              for (const n of entry.names) {
                const k = keyOf(n);
                if (k && !nameMap.has(k)) nameMap.set(k, entry);
              }
            } else {
              // merge tokens/names
              for (const v of names) if (existing.names.length < MAX_VARIANTS && !existing.names.includes(v)) existing.names.push(v);
              for (const tok of tokenSet) existing.tokenSet.add(tok);
              for (const n of names) {
                const k = keyOf(n);
                if (k && !nameMap.has(k)) nameMap.set(k, existing);
              }
            }
          }
        }
        // reset channel state
        cur = null; inDisp = false; dispChunks = []; dispLen = 0; dispTruncated = false;
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

    // mark programme presence
    for (const cid of programmesSeen) {
      const o = globalIdTo.get(cid);
      if (o) o.hasProgs = true;
    }
  }

  // optionally filter out channels with no programmes in any file
  if (EPG_REQUIRE_PROGS) {
    for (const [k, v] of nameMap.entries()) {
      const entry = v && v.id ? v : null;
      if (!entry || !entry.hasProgs) nameMap.delete(k);
    }
  }

  const kept = new Set([...nameMap.values()]).size;
  console.log(`EPG channels kept (Mexico-related ∩ scraped tokens): ${kept}`);
  return { nameMap, entries: [...new Set([...nameMap.values()])] };
}

// ---------- MATCH ----------
function jaccard(aTokens, bTokens) {
  const A = new Set(aTokens), B = new Set(bTokens);
  let inter = 0; for (const t of A) if (B.has(t)) inter++;
  return inter / (A.size + B.size - inter || 1);
}

function findMatch(channelName, nameKey, nameMap, entries) {
  const exact = nameMap.get(nameKey);
  if (exact) return { entry: exact, score: 1, method: 'exact' };

  const sTokArr = tokensOf(channelName);
  const sTok = new Set(sTokArr);

  // anchor: single strong token
  if (sTok.size === 1) {
    const [only] = [...sTok];
    for (const e of entries) if (e.tokenSet && e.tokenSet.has(only)) {
      return { entry: e, score: 0.99, method: 'anchor' };
    }
  }

  // subset: all scraped tokens within entry tokens; prefer smallest vocabulary
  let subsetBest = null, subsetBestSize = Infinity;
  for (const e of entries) {
    const E = e.tokenSet || new Set();
    let allIn = true;
    for (const t of sTok) { if (!E.has(t)) { allIn = false; break; } }
    if (allIn && E.size < subsetBestSize) { subsetBest = e; subsetBestSize = E.size; }
  }
  if (subsetBest) return { entry: subsetBest, score: 0.9, method: 'subset' };

  // fuzzy Jaccard
  let best = null, bestScore = 0;
  for (const e of entries) for (const nm of e.names) {
    const score = jaccard(sTokArr, tokensOf(nm));
    if (score > bestScore) { bestScore = score; best = e; }
  }
  if (best && bestScore >= FUZZY_MIN) return { entry: best, score: bestScore, method: 'fuzzy' };
  return { entry: null, score: 0, method: 'none' };
}

// ---------- DB ----------
async function saveRows(rows) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('Supabase env missing; skipped DB upload.');
    return;
  }
  if (!rows.length) {
    console.log('No rows to upload to Supabase.');
    return;
  }
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA }
  });

  // upsert by stream_url; fall back to insert if constraint missing
  let { error } = await supabase.from(SUPABASE_TABLE).upsert(rows, {
    onConflict: 'stream_url',
    ignoreDuplicates: false
  });
  if (error) {
    console.warn(`Upsert failed (${error.code ?? 'no-code'}): ${error.message}. Falling back to insert…`);
    ({ error } = await supabase.from(SUPABASE_TABLE).insert(rows));
  }
  if (error) console.warn(`Insert failed: ${error.message} (${error.code ?? 'no-code'})`);
  else console.log(`DB write OK: ${rows.length} rows`);
}

// ---------- MAIN ----------
async function ensureDir(p) { await fs.mkdir(p, { recursive: true }); }

async function main() {
  await ensureDir('out/mx');
  const browser = await chromium.launch({ headless: HEADLESS });
  try {
    console.log(`Scraping: ${SEARCH_URL}`);
    const links = await collectChannelPages(browser);
    console.log(`Found ${links.length} channel pages.`);
    const scraped = await scrapeAll(browser, links);
    console.log(`Channels with at least one .m3u8 (before probe): ${scraped.length}`);

    // probe streams
    for (const row of scraped) {
      const tested = [];
      for (const s of row.streams) {
        const ok = await probeM3U8(s.url);
        if (ok) tested.push(s);
      }
      row.streams = tested;
    }
    const filtered = scraped.filter(r => r.streams.length > 0);
    console.log(`Channels with at least one WORKING .m3u8: ${filtered.length}`);

    // build scraped token universe (for pruning EPG)
    const scrapedTokenUniverse = new Set();
    for (const r of filtered) for (const t of tokensOf(r.channelName)) scrapedTokenUniverse.add(t);

    const { nameMap, entries } = await parseManyEpgStreams(EPG_SOURCES, scrapedTokenUniverse);

    const records = [];
    const matchedOnly = [];
    for (const r of filtered) {
      const { entry, method } = findMatch(r.channelName, r.channelNameKey, nameMap, entries);
      for (const s of r.streams) {
        const rec = {
          stream_url: s.url,
          channel_guess: r.channelName,
          epg_channel_id: entry ? entry.id : null,
          epg_display_name: entry ? (entry.names[0] || null) : null,
          working: true,
          checked_at: new Date().toISOString()
        };
        records.push(rec);
        if (entry) matchedOnly.push({ ...rec, _match_method: method });      }
    }

    console.log(`Matched with EPG: ${matchedOnly.length} stream rows (across ${filtered.length} channels).`);

    await fs.writeFile(path.join('out', 'mx', 'records.json'), JSON.stringify(records, null, 2), 'utf8');
    await fs.writeFile(path.join('out', 'mx', 'matches.json'), JSON.stringify(matchedOnly, null, 2), 'utf8');

    if (LOG_UNMATCHED) {
      const matchedUrls = new Set(matchedOnly.map(x => x.stream_url));
      const unmatched = records.filter(x => !matchedUrls.has(x.stream_url));
      await fs.writeFile(path.join('out', 'mx', 'unmatched.json'), JSON.stringify(unmatched, null, 2), 'utf8');
      console.log(`Wrote out/mx/unmatched.json with ${unmatched.length} unmatched rows`);
    }

    await saveRows(records);
  } finally {
    await browser.close();
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
