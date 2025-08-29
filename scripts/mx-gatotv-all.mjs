// scripts/mx-gatotv-all.mjs
// GatoTV-only EPG scraper (24h window, no XMLTV).
// - Opens directory page
// - Iterates each /canal/... link (sequentially)
// - Opens channel page (today) + dated page for tomorrow (if available)
// - Scrapes table with headers like "Hora Inicio / Hora Fin / Programa"
// - Converts to UTC, clamps to 24h from now, upserts into epg_programs on (channel_id, start_ts)
// - EXTRAS now includes: channel_name_normalized, country, country_normalized

import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { DateTime } from 'luxon';
import { createClient } from '@supabase/supabase-js';

// ---------- ENV ----------
const GATOTV_DIR_URL       = process.env.GATOTV_DIR_URL || 'https://www.gatotv.com/canales_de_tv';
const GATOTV_TZ            = process.env.GATOTV_TZ || 'America/Mexico_City';
const PROGRAMS_HOURS_AHEAD = Number(process.env.PROGRAMS_HOURS_AHEAD || '24');
const GATOTV_MAX_CHANNELS  = Number(process.env.GATOTV_MAX_CHANNELS || '0'); // 0 = all
const HEADLESS             = (process.env.HEADLESS ?? 'true') !== 'false';
const PER_PAGE_DELAY_MS    = Number(process.env.PER_PAGE_DELAY_MS || '200');
const NAV_TIMEOUT_MS       = Number(process.env.NAV_TIMEOUT_MS || '25000');

const SUPABASE_URL         = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA      = process.env.SUPABASE_SCHEMA || 'public';
const PROGRAMS_TABLE       = process.env.PROGRAMS_TABLE || 'epg_programs';

// ---------- HELPERS ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const norm  = (s) => String(s ?? '').replace(/\s+/g, ' ').trim();
const lower = (s) => norm(s).toLowerCase();

// diacritic strip for normalization
function stripAccents(s) {
  return String(s || '').normalize('NFD').replace(/\p{Diacritic}+/gu, '');
}

// extract trailing country and normalized base channel name from something like:
// "Canal A&E (Perú)" → { base: "Canal A&E", country: "Perú", base_norm: "canal a&e", country_norm: "peru" }
// If no trailing "(...)" exists, country fields are null and base_norm normalizes whole string.
function extractNameParts(name) {
  const raw = norm(name);
  const m = raw.match(/\(([^)]*)\)\s*$/);
  let base = raw;
  let country = null;
  if (m && m[1]) {
    country = norm(m[1]);
    base = norm(raw.replace(/\(([^)]*)\)\s*$/, ''));
  }
  const base_norm    = lower(stripAccents(base));
  const country_norm = country ? lower(stripAccents(country)) : null;
  return { base, country, base_norm, country_norm };
}

function parseTimeLocalToUTC(localDateISO, timeStr, tz) {
  // Accept: "07:00", "7:00", "07:00 hrs", "7:00 h", "7:00 am", "19:30"
  let s = lower(timeStr).replace(/\./g,'').replace(/\bhrs?\b/g,'').replace(/\bh\b/g,'').trim();
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return null;
  let hh = +m[1], mm = +m[2];
  if (/\b(am|pm)\b/.test(s)) {
    const pm = /\bpm\b/.test(s);
    if (pm && hh < 12) hh += 12;
    if (!pm && hh === 12) hh = 0;
  }
  const t = `${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:00`;
  return DateTime.fromISO(`${localDateISO}T${t}`, { zone: tz }).toUTC().toISO();
}

function clamp24h(programs, nowUTC, hoursAhead) {
  const end = nowUTC.plus({ hours: hoursAhead });
  return programs
    .filter(p => DateTime.fromISO(p.start_ts) < end)
    .map(p => ({
      ...p,
      stop_ts: DateTime.fromISO(p.stop_ts) > end ? end.toISO() : p.stop_ts
    }));
}

// ---------- DB ----------
function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

async function savePrograms(rows) {
  if (!rows.length) return;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('No Supabase creds; skip');
    return;
  }
  const client = sb();
  const B = 500;
  for (let i = 0; i < rows.length; i += B) {
    const slice = rows.slice(i, i + B);
    let { error } = await client.from(PROGRAMS_TABLE)
      .upsert(slice, { onConflict: 'channel_id, start_ts' });
    if (error && /no unique|no exclusion/i.test(error.message || '')) {
      ({ error } = await client.from(PROGRAMS_TABLE).insert(slice));
    }
    if (error) { console.warn(`Programs batch failed: ${error.message}`); break; }
  }
  console.log(`Program ingest attempted: ${rows.length}`);
}

// ---------- SCRAPING (Playwright, DOM-based) ----------
async function getDirectoryLinks(page) {
  await page.goto(GATOTV_DIR_URL, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
  await page.waitForTimeout(400);
  // Collect every anchor to /canal/..., de-dup by absolute URL
  const links = await page.$$eval('a[href*="/canal/"]', as => {
    const seen = new Set(), out = [];
    for (const a of as) {
      const href = a.getAttribute('href') || '';
      if (!href.includes('/canal/')) continue;
      let url;
      try { url = new URL(href, location.href).href; } catch { continue; }
      if (seen.has(url)) continue;
      const name = (a.textContent || '').replace(/\s+/g,' ').trim();
      if (!name) continue;
      seen.add(url);
      out.push({ name, url });
    }
    return out;
  });
  return links;
}

// Map header indices by text content
function mapHeaderIndices(headers) {
  // headers: array of lowercased header texts
  const idx = { start: -1, end: -1, title: -1 };
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (h.includes('hora inicio') || h.includes('inicio')) idx.start = i;
    else if (h.includes('hora fin') || h.includes('fin') || h.includes('término') || h.includes('termino')) idx.end = i;
    else if (h.includes('programa') || h.includes('título') || h.includes('titulo')) idx.title = i;
  }
  // fallbacks if ambiguous
  if (idx.start === -1 && headers.length >= 1) idx.start = 0;
  if (idx.end   === -1 && headers.length >= 2) idx.end   = 1;
  if (idx.title === -1 && headers.length >= 3) idx.title = 2;
  return idx;
}

async function scrapeChannelTable(page, url) {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
  await page.waitForTimeout(200);

  const tableInfo = await page.evaluate(() => {
    const norm = s => String(s||'').replace(/\s+/g,' ').trim().toLowerCase();
    const text = el => (el?.textContent || '').replace(/\s+/g,' ').trim();

    const tables = Array.from(document.querySelectorAll('table'));
    const out = [];
    for (const tbl of tables) {
      // headers
      let headerCells = Array.from(tbl.querySelectorAll('thead tr th'));
      if (!headerCells.length) headerCells = Array.from(tbl.querySelectorAll('tr th'));
      if (!headerCells.length) headerCells = Array.from(tbl.querySelectorAll('tr:first-child td'));
      const headers = headerCells.map(th => norm(th.textContent));
      if (!headers.length) continue;

      const joined = headers.join(' ');
      if (!(joined.includes('hora') || joined.includes('programa') || joined.includes('título') || joined.includes('titulo'))) {
        continue;
      }

      // body rows
      const bodyRows = Array.from(tbl.querySelectorAll('tbody tr'));
      const allRows  = bodyRows.length ? bodyRows : Array.from(tbl.querySelectorAll('tr')).slice(1);
      const rows = allRows.map(tr => Array.from(tr.querySelectorAll('td')).map(td => text(td)));
      out.push({ headers, rows });
    }
    return out;
  });

  if (!tableInfo.length) return [];

  // pick the table with most data rows
  tableInfo.sort((a,b) => b.rows.length - a.rows.length);
  const chosen = tableInfo[0];
  const idx = mapHeaderIndices(chosen.headers);

  const rows = [];
  for (const r of chosen.rows) {
    if (!r.length) continue;
    const startLocal = r[idx.start] || '';
    const stopLocal  = r[idx.end]   || '';
    const title      = r[idx.title] || '';
    if (!title || !startLocal) continue;
    rows.push({ startLocal, stopLocal: stopLocal || null, title });
  }
  return rows;
}

async function fetchChannelDay(page, baseUrl, localISO) {
  // Many pages accept /YYYY-MM-DD; if 404 or empty, we already got "today" from base.
  const dated = baseUrl.endsWith(`/${localISO}`) ? baseUrl : `${baseUrl.replace(/\/$/,'')}/${localISO}`;
  try {
    const rows = await scrapeChannelTable(page, dated);
    return { url: dated, rows };
  } catch {
    return { url: dated, rows: [] };
  }
}

function materializeDay(rows, localISO, tz) {
  // Turn (start[, stop], title) into UTC spans; infer stop from next start or +60m; guard for non-increasing times
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const start = parseTimeLocalToUTC(localISO, r.startLocal, tz);
    if (!start) continue;
    let stop = null;
    if (r.stopLocal) stop = parseTimeLocalToUTC(localISO, r.stopLocal, tz);
    else if (rows[i + 1]?.startLocal) stop = parseTimeLocalToUTC(localISO, rows[i + 1].startLocal, tz);
    if (!stop) stop = DateTime.fromISO(start).plus({ minutes: 60 }).toISO();
    if (DateTime.fromISO(stop) <= DateTime.fromISO(start)) {
      stop = DateTime.fromISO(start).plus({ minutes: 30 }).toISO();
    }
    out.push({ title: r.title, start_ts: start, stop_ts: stop });
  }
  return out;
}

// ---------- MAIN ----------
async function main() {
  await fs.mkdir('out/mx', { recursive: true });

  const browser = await chromium.launch({
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({
    'User-Agent': 'Mozilla/5.0 (compatible; GatoTV-EPG/1.0)',
    'Accept-Language': 'es-MX,es;q=0.9,en;q=0.5',
  });

  let debugSaved = 0;
  try {
    // 1) Directory → channel links
    let channels = await getDirectoryLinks(page);
    if (GATOTV_MAX_CHANNELS > 0 && channels.length > GATOTV_MAX_CHANNELS) {
      channels = channels.slice(0, GATOTV_MAX_CHANNELS);
    }
    await fs.writeFile(path.join('out','mx','gatotv_directory.json'), JSON.stringify(channels, null, 2), 'utf8');
    console.log(`GatoTV directory channels: ${channels.length}`);

    // 2) Build 24h window
    const nowUTC      = DateTime.utc();
    const localNow    = nowUTC.setZone(GATOTV_TZ);
    const todayISO    = localNow.toISODate();
    const tomorrowISO = localNow.plus({ days: 1 }).toISODate();

    const programs = [];

    // 3) Sequential: open URL → scrape today → optionally scrape tomorrow → return → repeat
    for (const ch of channels) {
      // Parse name parts for extras (channel_name_normalized + country fields)
      const parts = extractNameParts(ch.name);

      // Base page (today)
      const rowsToday = await scrapeChannelTable(page, ch.url);
      const a1 = materializeDay(rowsToday, todayISO, GATOTV_TZ);

      // If not enough coverage, try dated tomorrow
      const { rows: rowsTomorrow } = await fetchChannelDay(page, ch.url, tomorrowISO);
      const a2 = materializeDay(rowsTomorrow, tomorrowISO, GATOTV_TZ);

      const clamped = clamp24h(a1.concat(a2), nowUTC, PROGRAMS_HOURS_AHEAD);

      if (!clamped.length && debugSaved < 3) {
        try {
          const html = await page.content();
          await fs.writeFile(path.join('out','mx',`debug_${debugSaved+1}.html`), html, 'utf8');
          debugSaved++;
        } catch {}
      }

      for (const r of clamped) {
        programs.push({
          // Use the GatoTV channel page URL as the channel_id for now; you can remap in SQL later
          channel_id: ch.url,
          start_ts: r.start_ts,
          stop_ts: r.stop_ts,
          title: r.title,
          sub_title: null,
          summary: null,
          categories: [],
          program_url: null,
          episode_num_xmltv: null,
          icon_url: null,
          rating: null,
          star_rating: null,
          season: null,
          episode: null,
          language: 'es',
          orig_language: 'es',
          credits: null,
          premiere: false,
          previously_shown: false,
          // >>> NEW: richer extras with normalized name + country <<<
          extras: {
            source: 'gatotv',
            channel_name: ch.name,
            channel_name_normalized: parts.base_norm,  // e.g., "canal a&e"
            country: parts.country,                    // e.g., "Perú"
            country_normalized: parts.country_norm     // e.g., "peru"
          },
          ingested_at: DateTime.utc().toISO(),
          source_epg: 'gatotv',
          source_url: ch.url
        });
      }

      await sleep(PER_PAGE_DELAY_MS);
    }

    await fs.writeFile(
      path.join('out', 'mx', 'epg_programs_sample.json'),
      JSON.stringify(programs.slice(0, 250), null, 2),
      'utf8'
    );

    await savePrograms(programs);
    console.log(`GatoTV 24h ingest complete. Channels scraped: ${channels.length}, programs: ${programs.length}`);
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
