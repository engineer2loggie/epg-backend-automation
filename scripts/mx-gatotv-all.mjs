// scripts/mx-gatotv-all.mjs
// GatoTV directory → visit every /canal/... page → scrape schedule for today + spillover to cover 24h
// → convert to UTC → upsert into epg_programs (on channel_id, start_ts).
// No XMLTV, no VPN, no matching inside the script (you'll do SQL-side if you want).

import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { createClient } from '@supabase/supabase-js';
import { DateTime } from 'luxon';

// ---------- ENV ----------
const GATOTV_DIR_URL = process.env.GATOTV_DIR_URL || 'https://www.gatotv.com/canales_de_tv';
const GATOTV_TZ = process.env.GATOTV_TZ || 'America/Mexico_City';
const PROGRAMS_HOURS_AHEAD = Number(process.env.PROGRAMS_HOURS_AHEAD || '24');
const GATOTV_MAX_CHANNELS = Number(process.env.GATOTV_MAX_CHANNELS || '0'); // 0 = all

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const PROGRAMS_TABLE = process.env.PROGRAMS_TABLE || 'epg_programs';

const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const UA = process.env.SCRAPER_UA || 'Mozilla/5.0 (compatible; GatoTV-EPG/1.0)';

// ---------- tiny utils (no regex literals) ----------
function collapseSpaces(s) {
  let out = '', inSpace = false;
  for (const ch of String(s)) {
    const sp = ch === ' ' || ch === '\n' || ch === '\r' || ch === '\t' || ch === '\f';
    if (sp) {
      if (!inSpace) { out += ' '; inSpace = true; }
    } else {
      out += ch; inSpace = false;
    }
  }
  return out.trim();
}

function normalizeLower(s) {
  return collapseSpaces(String(s).toLowerCase()).replaceAll('.', '');
}

function looksLikeTime(s) {
  const t = normalizeLower(s);
  const i = t.indexOf(':');
  if (i <= 0) return false;
  const hhRaw = t.slice(Math.max(0, i - 2), i).trim();
  const mmRaw = t.slice(i + 1, i + 3);
  if (hhRaw.length < 1 || hhRaw.length > 2) return false;
  const hh = Number(hhRaw), mm = Number(mmRaw);
  if (!Number.isInteger(hh) || !Number.isInteger(mm)) return false;
  if (hh < 0 || hh > 23) return false; // 12 will be corrected if am/pm present
  if (mm < 0 || mm > 59) return false;
  return true;
}

function parseLocalTimeToUTC(localDateISO, timeStr, tz) {
  const t = normalizeLower(timeStr);
  const i = t.indexOf(':');
  if (i <= 0) return null;
  let hh = Number(t.slice(Math.max(0, i - 2), i).trim());
  const mm = Number(t.slice(i + 1, i + 3));
  const hasAM = t.includes('am'), hasPM = t.includes('pm');
  if (hasAM || hasPM) {
    if (hasPM && hh < 12) hh += 12;
    if (hasAM && hh === 12) hh = 0;
  }
  const hStr = String(hh).padStart(2, '0');
  const mStr = String(mm).padStart(2, '0');
  return DateTime.fromISO(`${localDateISO}T${hStr}:${mStr}:00`, { zone: tz }).toUTC().toISO();
}

function clamp24h(programs, nowUTC, hours) {
  const end = nowUTC.plus({ hours });
  const out = [];
  for (const p of programs) {
    const s = DateTime.fromISO(p.start_ts);
    const e = DateTime.fromISO(p.stop_ts);
    if (s >= end) continue;
    out.push({ ...p, stop_ts: e > end ? end.toISO() : p.stop_ts });
  }
  return out;
}

// ---------- directory scrape (Playwright; NO regex) ----------
async function extractDirectoryChannels(page) {
  await page.goto(GATOTV_DIR_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(400);
  // Grab all anchors that link to /canal/...
  const links = await page.$$eval('a[href*="/canal/"]', (as) => {
    const seen = new Set();
    const out = [];
    for (const a of as) {
      const href = a.getAttribute('href') || '';
      if (!href.includes('/canal/')) continue;
      let url;
      try { url = new URL(href, location.href).href; } catch { continue; }
      if (seen.has(url)) continue;
      seen.add(url);
      const txt = (a.textContent || '').trim().replace(/\s+/g, ' ');
      if (!txt) continue;
      out.push({ name: txt, url });
    }
    return out;
  });
  return links;
}

// ---------- schedule scrape (Playwright; NO regex) ----------
function pickCellsAsRow(tds) {
  // Find first two time-looking cells and a title cell after them
  let startIdx = -1, endIdx = -1;
  for (let i = 0; i < Math.min(5, tds.length); i++) {
    if (startIdx === -1 && looksLikeTime(tds[i])) { startIdx = i; continue; }
    if (startIdx !== -1 && endIdx === -1 && looksLikeTime(tds[i])) { endIdx = i; break; }
  }
  if (startIdx === -1) return null;
  let titleIdx = -1;
  for (let i = Math.max(endIdx, startIdx) + 1; i < tds.length; i++) {
    const txt = collapseSpaces(tds[i]);
    if (txt) { titleIdx = i; break; }
  }
  if (titleIdx === -1) titleIdx = tds.length - 1;
  const startLocal = collapseSpaces(tds[startIdx] || '');
  const stopLocal = endIdx !== -1 ? collapseSpaces(tds[endIdx] || '') : null;
  const title = collapseSpaces(tds[titleIdx] || '');
  if (!startLocal || !title) return null;
  return { startLocal, stopLocal, title };
}

async function scrapeChannelDay(page, baseUrl, localISO) {
  let url = baseUrl;
  if (!/\/\d{4}-\d{2}-\d{2}$/.test(baseUrl)) url = baseUrl.replace(/\/?$/, `/${localISO}`);
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(200);

  // Prefer table rows
  const rows = await page.$$eval('tr', (trs) => {
    const normalize = (s) => s.replace(/\s+/g, ' ').trim();
    const out = [];
    for (const tr of trs) {
      const tds = Array.from(tr.querySelectorAll('td')).map((td) => normalize(td.textContent || ''));
      if (tds.length >= 2) out.push(tds);
    }
    return out;
  });

  const parsed = [];
  for (const tds of rows) {
    const row = pickCellsAsRow(tds);
    if (row) parsed.push(row);
  }

  // If no rows detected, try “.tabla .row” style fallback (still DOM; no regex)
  if (parsed.length === 0) {
    const blocks = await page.$$eval('div, li, p, section', (nodes) => {
      const normalize = (s) => s.replace(/\s+/g, ' ').trim();
      return nodes.map((n) => normalize(n.textContent || '')).filter(Boolean);
    });
    // Heuristic: adjacent blocks where first looks like time, next looks like title
    for (let i = 0; i < blocks.length - 1; i++) {
      const a = blocks[i], b = blocks[i + 1];
      // The “looksLikeTime” logic will run on Node side
      parsed.push({ startLocal: a, stopLocal: null, title: b });
    }
  }

  return { url, rows: parsed };
}

function materializeDay(rows, localISO, tz) {
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (!looksLikeTime(r.startLocal)) continue;
    const start = parseLocalTimeToUTC(localISO, r.startLocal, tz);
    if (!start) continue;
    let stop = null;
    if (r.stopLocal && looksLikeTime(r.stopLocal)) {
      stop = parseLocalTimeToUTC(localISO, r.stopLocal, tz);
    } else if (rows[i + 1]?.startLocal && looksLikeTime(rows[i + 1].startLocal)) {
      stop = parseLocalTimeToUTC(localISO, rows[i + 1].startLocal, tz);
    }
    if (!stop) stop = DateTime.fromISO(start).plus({ minutes: 60 }).toISO();
    if (DateTime.fromISO(stop) <= DateTime.fromISO(start)) {
      stop = DateTime.fromISO(start).plus({ minutes: 30 }).toISO();
    }
    out.push({ title: r.title, start_ts: start, stop_ts: stop });
  }
  return out;
}

// ---------- DB ----------
function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

async function savePrograms(programs) {
  if (!programs.length) return;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log('No Supabase creds; skip'); return;
  }
  const client = sb();
  const BATCH = 500;
  for (let i = 0; i < programs.length; i += BATCH) {
    const slice = programs.slice(i, i + BATCH);
    let { error } = await client
      .from(PROGRAMS_TABLE)
      .upsert(slice, { onConflict: 'channel_id, start_ts' });
    if (error && /no unique|no exclusion/i.test(error.message || '')) {
      ({ error } = await client.from(PROGRAMS_TABLE).insert(slice));
    }
    if (error) { console.warn(`Programs batch failed: ${error.message}`); break; }
  }
  console.log(`Program ingest attempted: ${programs.length}`);
}

// ---------- MAIN ----------
async function main() {
  await fs.mkdir('out/mx', { recursive: true });

  const browser = await chromium.launch({
    headless: HEADLESS,
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });

  try {
    const page = await browser.newPage({ userAgent: UA });

    // 1) Directory → channel links (no regex; DOM only)
    const channels = await extractDirectoryChannels(page);
    let picked = channels;
    if (GATOTV_MAX_CHANNELS > 0 && channels.length > GATOTV_MAX_CHANNELS) {
      picked = channels.slice(0, GATOTV_MAX_CHANNELS);
    }
    await fs.writeFile(
      path.join('out', 'mx', 'gatotv_directory.json'),
      JSON.stringify(picked, null, 2),
      'utf8'
    );
    console.log(`GatoTV directory channels: ${picked.length}`);

    // 2) Today + tomorrow → clamp to 24h window
    const nowUTC = DateTime.utc();
    const localNow = nowUTC.setZone(GATOTV_TZ);
    const todayISO = localNow.toISODate();
    const tomorrowISO = localNow.plus({ days: 1 }).toISODate();

    const programs = [];
    // Reuse the same page to keep it light
    for (const ch of picked) {
      const d1 = await scrapeChannelDay(page, ch.url, todayISO);
      const d2 = await scrapeChannelDay(page, ch.url, tomorrowISO);
      const a1 = materializeDay(d1.rows, todayISO, GATOTV_TZ);
      const a2 = materializeDay(d2.rows, tomorrowISO, GATOTV_TZ);
      const combined = a1.concat(a2);
      const clamped = clamp24h(combined, nowUTC, PROGRAMS_HOURS_AHEAD);
      for (const r of clamped) {
        programs.push({
          channel_id: ch.url,   // using GatoTV URL as ID for now; do SQL matching later if desired
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
          extras: { source: 'gatotv', channel_name: ch.name },
          ingested_at: DateTime.utc().toISO()
        });
      }
    }

    await fs.writeFile(
      path.join('out', 'mx', 'epg_programs_sample.json'),
      JSON.stringify(programs.slice(0, 250), null, 2),
      'utf8'
    );
    await savePrograms(programs);
    console.log(`GatoTV 24h ingest complete. Channels scraped: ${picked.length}, programs: ${programs.length}`);
  } finally {
    await browser.close();
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
