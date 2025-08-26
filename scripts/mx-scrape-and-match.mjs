// scripts/mx-scrape-and-match.mjs
// Scrape iptv-org.github.io (live MX) -> channel pages -> Streams (m3u8 only)
// Download EPGShare MX and fuzzy-match by display-name -> upsert to Supabase

import { chromium } from 'playwright';
import { XMLParser } from 'fast-xml-parser';
import zlib from 'node:zlib';
import { setTimeout as delay } from 'node:timers/promises';
import { createClient } from '@supabase/supabase-js';

// -------- Config (env overrides allowed) --------
const SEARCH_URL = process.env.MX_SEARCH_URL || 'https://iptv-org.github.io/?q=live%20country:MX';
const EPG_GZ_URL = process.env.MX_EPG_URL || 'https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz';

const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const MAX_CHANNELS = Number(process.env.MAX_CHANNELS || '0'); // 0 = no cap
const PER_PAGE_DELAY_MS = Number(process.env.PER_PAGE_DELAY_MS || '150'); // politeness delay
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS || '30000');

// Supabase (optional)
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'epg_streams';

// -------- Helpers --------
function norm(s) {
  if (!s) return '';
  return s
    .normalize('NFD').replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/\b(tv|canal|hd)\b/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function uniqueBy(arr, keyFn) {
  const m = new Map();
  for (const x of arr) {
    const k = keyFn(x);
    if (!m.has(k)) m.set(k, x);
  }
  return [...m.values()];
}

async function fetchBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Fetch failed ${r.status} ${url}`);
  return Buffer.from(await r.arrayBuffer());
}

// -------- 1) Scrape list page for channel page links --------
async function collectChannelPages(browser) {
  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT_MS);
  await page.goto(SEARCH_URL, { waitUntil: 'domcontentloaded' });

  // Give the client-side filter a moment to render rows
  await page.waitForTimeout(1500);

  // Grab channel cards/rows that link to channel pages.
  // We look for anchors whose href includes "/channels/" (site uses static pages per channel).
  let items = await page.$$eval('a[href*="/channels/"]', (as) => {
    // Collect a few attributes per anchor; find an image nearby if present
    const out = [];
    for (const a of as) {
      const href = a.getAttribute('href') || '';
      // Narrow to channel pages, skip external
      if (!href.includes('/channels/')) continue;

      // Channel name shown in list (text content of the link)
      const name = (a.textContent || '').trim();

      // Try to find a nearby <img> (logo) in the same card/row
      let logo = null;
      const row = a.closest('tr,li,article,div') || a.parentElement;
      if (row) {
        const img = row.querySelector('img');
        if (img) logo = img.src || img.getAttribute('src');
      }

      // Absolute URL
      const url = new URL(href, location.href).href;
      out.push({ url, name, logo });
    }
    // Deduplicate by URL
    const map = new Map();
    for (const it of out) if (!map.has(it.url)) map.set(it.url, it);
    return [...map.values()];
  });

  // Occasionally the search page keeps hidden duplicates; remove exact dupes
  items = uniqueBy(items, (x) => x.url);

  // Optional cap to keep runs short
  if (MAX_CHANNELS > 0 && items.length > MAX_CHANNELS) {
    items = items.slice(0, MAX_CHANNELS);
  }

  await page.close();
  return items;
}

// -------- 2) Visit each channel page, extract .m3u8 links (Streams tab/section) --------
async function scrapeChannelStreams(browser, items) {
  const out = [];
  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT_MS);

  let i = 0;
  for (const it of items) {
    i++;
    try {
      await page.goto(it.url, { waitUntil: 'domcontentloaded' });
      // small delay to let any client rendering complete
      await page.waitForTimeout(600);

      // Try to click a "Streams" tab if present (best-effort)
      const streamsTab = await page.$('text=Streams');
      if (streamsTab) {
        await streamsTab.click().catch(() => {});
        await page.waitForTimeout(400);
      }

      // Collect clickable .m3u8 anchors
      let anchors = await page.$$eval('a[href*=".m3u8"]', (els) =>
        els.map((e) => ({
          url: e.href,
          text: (e.textContent || '').trim()
        }))
      );

      // Fallback: scrape raw text for .m3u8 URLs if anchors aren’t present
      if (!anchors.length) {
        const html = await page.content();
        const rx = /https?:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi;
        const found = new Set();
        let m;
        while ((m = rx.exec(html))) found.add(m[0]);
        anchors = [...found].map((u) => ({ url: u, text: '' }));
      }

      // Only keep plausible m3u8s
      anchors = uniqueBy(
        anchors.filter((a) => /^https?:\/\//i.test(a.url)),
        (a) => a.url
      );

      if (anchors.length) {
        out.push({
          channelName: it.name,
          channelNameNorm: norm(it.name),
          channelPage: it.url,
          logo: it.logo || null,
          streams: anchors.map((a) => ({
            url: a.url,
            quality: (a.text.match(/\b(1080p|720p|480p|360p|HD|SD)\b/i) || [])[0] || null
          }))
        });
      }
    } catch (e) {
      console.error(`Error scraping ${it.url}: ${e.message}`);
    }
    await delay(PER_PAGE_DELAY_MS);
  }

  await page.close();
  return out;
}

// -------- 3) Download + parse EPGShare MX, build display-name map --------
async function parseEpgMx() {
  console.log(`Downloading EPGShare MX… ${EPG_GZ_URL}`);
  const gz = await fetchBuffer(EPG_GZ_URL);
  const xmlBuf = zlib.gunzipSync(gz);
  const xml = xmlBuf.toString('utf8');

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '',
    allowBooleanAttributes: true,
    trimValues: true
  });
  const doc = parser.parse(xml);

  const channels = (doc.tv && doc.tv.channel) ? (Array.isArray(doc.tv.channel) ? doc.tv.channel : [doc.tv.channel]) : [];
  const progs = (doc.tv && doc.tv.programme) ? (Array.isArray(doc.tv.programme) ? doc.tv.programme : [doc.tv.programme]) : [];

  // Map: normalized display-name -> { id, names[], icon, hasProgrammes }
  const nameMap = new Map();
  const idToObj = new Map();

  for (const ch of channels) {
    const id = ch.id;
    const namesArr = [];
    if (Array.isArray(ch['display-name'])) {
      for (const dn of ch['display-name']) if (dn && typeof dn === 'string') namesArr.push(dn);
    } else if (typeof ch['display-name'] === 'string') {
      namesArr.push(ch['display-name']);
    }
    const icon = ch.icon?.src || null;

    const obj = { id, names: namesArr, icon, hasProgrammes: false };
    idToObj.set(id, obj);
    for (const n of namesArr) {
      const k = norm(n);
      if (k) {
        if (!nameMap.has(k)) nameMap.set(k, []);
        nameMap.get(k).push(obj);
      }
    }
  }

  for (const p of progs) {
    const chId = p.channel;
    const obj = idToObj.get(chId);
    if (obj) obj.hasProgrammes = true;
  }

  const withPrograms = [...idToObj.values()].filter((x) => x.hasProgrammes);
  console.log(`EPG channels: ${channels.length}, with programmes for ${withPrograms.length}`);
  return { nameMap, idToObj };
}

// -------- 4) Simple matcher: exact normalized name --------
function matchScrapedToEpg(scraped, nameMap) {
  const matches = [];
  for (const s of scraped) {
    const k = s.channelNameNorm;
    const candidates = nameMap.get(k) || [];
    if (candidates.length) {
      // prefer the first candidate
      matches.push({
        channelName: s.channelName,
        logo: s.logo,
        streams: s.streams,
        epgChannelId: candidates[0].id,
        epgDisplayNames: candidates[0].names,
        epgIcon: candidates[0].icon || null
      });
    }
  }
  return matches;
}

// -------- 5) Optional: upsert to Supabase --------
async function uploadToSupabase(rows) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('Supabase env missing or no rows; skipped DB upload.');
    return;
  }
  if (!rows.length) {
    console.log('No rows to upload to Supabase.');
    return;
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
  // Expecting a table like:
  // create table epg_streams (
  //   country text,
  //   channel_name text,
  //   channel_logo text,
  //   channel_page text,
  //   stream_url text,
  //   stream_quality text,
  //   epg_channel_id text,
  //   epg_display_names jsonb,
  //   epg_icon text,
  //   inserted_at timestamptz default now(),
  //   primary key (country, channel_name, stream_url)
  // );
  const payload = [];
  for (const r of rows) {
    for (const s of r.streams) {
      payload.push({
        country: 'MX',
        channel_name: r.channelName,
        channel_logo: r.logo,
        channel_page: r.channelPage || null,
        stream_url: s.url,
        stream_quality: s.quality,
        epg_channel_id: r.epgChannelId,
        epg_display_names: r.epgDisplayNames,
        epg_icon: r.epgIcon
      });
    }
  }

  const { error } = await supabase.from(SUPABASE_TABLE).upsert(payload, {
    ignoreDuplicates: false,
    onConflict: 'country,channel_name,stream_url'
  });
  if (error) throw error;
  console.log(`Supabase upsert done: ${payload.length} rows`);
}

// -------- Main --------
(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  try {
    console.log(`Scraping: ${SEARCH_URL}`);
    const list = await collectChannelPages(browser);
    console.log(`Found ${list.length} channel pages.`);

    const scraped = await scrapeChannelStreams(browser, list);
    console.log(`Channels with at least one .m3u8: ${scraped.length}`);

    const { nameMap } = await parseEpgMx();
    const matched = matchScrapedToEpg(scraped, nameMap);
    console.log(`Matched ${matched.length} channels with working streams & EPG.`);

    // Save artifact
    await BunOrNodeWriteFile('out/mx/matches.json', JSON.stringify(matched, null, 2));
    console.log('Wrote out/mx/matches.json');

    // Push to Supabase (optional)
    await uploadToSupabase(matched);
  } finally {
    await browser.close();
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Node/Bun-compatible write helper
async function BunOrNodeWriteFile(p, s) {
  const fs = await import('node:fs/promises');
  const path = await import('node:path');
  await fs.mkdir(path.dirname(p), { recursive: true });
  await fs.writeFile(p, s);
}

