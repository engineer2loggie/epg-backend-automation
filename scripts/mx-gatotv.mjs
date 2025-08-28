// scripts/mx-gatotv-all.mjs
// Crawl GatoTV directory → visit every /canal/... page → scrape today's + tomorrow's schedule
// → convert to UTC and clamp to a 24h window → upsert into epg_programs on (channel_id, start_ts).
// No matching, no VPN, no XMLTV.

import fs from 'node:fs/promises';
import path from 'node:path';
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

const UA = process.env.SCRAPER_UA || 'Mozilla/5.0 (compatible; GatoTV-EPG/1.0)';

// ---------- HELPERS ----------
function cleanText(html) {
  return String(html).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

async function fetchHtml(url) {
  const r = await fetch(url, { headers: { 'user-agent': UA } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return await r.text();
}

// Extract ALL /canal/... anchors from the directory page (left column list and elsewhere)
function extractDirectoryChannels(html) {
  const out = [];
  const re = /<a\s+[^>]*href=["'](\/canal\/[^"'#?]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const seen = new Set();
  let m;
  while ((m = re.exec(html))) {
    const rel = m[1];
    const name = cleanText(m[2]);
    const url = new URL(rel, GATOTV_DIR_URL).href;
    if (!name || seen.has(url)) continue;
    seen.add(url);
    out.push({ name, url });
  }
  return out;
}

// Try a table-driven parse first: <tr> → <td> cells for start/end/title
function parseScheduleTableRows(html) {
  const rows = [];
  const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let m;
  while ((m = rowRe.exec(html))) {
    const row = m[1];
    const tds = [];
    const tdRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let k;
    while ((k = tdRe.exec(row))) tds.push(k[1]);
    if (tds.length < 2) continue;

    // Find first two time-like cells (Hora Inicio / Hora Fin)
    const isTime = (s) => /\b\d{1,2}:\d{2}\b/.test(cleanText(s));
    let idxStart = -1, idxEnd = -1;
    for (let i = 0; i < Math.min(4, tds.length); i++) {
      if (idxStart === -1 && isTime(tds[i])) { idxStart = i; continue; }
      if (idxStart !== -1 && idxEnd === -1 && isTime(tds[i])) { idxEnd = i; break; }
    }
    if (idxStart === -1) continue;

    // Title = first non-empty text cell after the end time (or after start if no end)
    let titleIdx = -1;
    for (let i = Math.max(idxEnd, idxStart) + 1; i < tds.length; i++) {
      const txt = cleanText(tds[i]);
      if (txt) { titleIdx = i; break; }
    }
    if (titleIdx === -1) titleIdx = tds.length - 1;

    const startLocal = cleanText(tds[idxStart] || '');
    const stopLocal = idxEnd !== -1 ? cleanText(tds[idxEnd] || '') : null;
    const title = cleanText(tds[titleIdx] || '');
    if (!startLocal || !title) continue;

    rows.push({ startLocal, stopLocal, title });
  }
  return dedupeRows(rows);
}

// Fallback: heuristic pattern "start [- end] ... <tag>Title</tag>"
function parseScheduleHeuristic(html) {
  const rows = [];
  const cleaned = html
    .replace(/\r|\n/g, ' ')
    .replace(/<\s*br\s*\/?>(?=\S)/gi, ' ')
    .replace(/\s+/g, ' ');
  const rx =
    /(\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?)\s*-?\s*(\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?)?[^>]*?<[^>]*?>([^<]{2,200})/gi;
  const seen = new Set();
  let m;
  while ((m = rx.exec(cleaned))) {
    const start = (m[1] || '').trim();
    const stop = (m[2] || '').trim() || null;
    const title = (m[3] || '').trim();
    const key = `${start}|${stop}|${title}`;
    if (!start || !title || seen.has(key)) continue;
    seen.add(key);
    rows.push({ startLocal: start, stopLocal: stop, title });
  }
  return rows;
}

function dedupeRows(rows) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const key = `${r.startLocal}|${r.stopLocal || ''}|${r.title}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

function parseSchedule(html) {
  const table = parseScheduleTableRows(html);
  if (table.length) return table;
  return parseScheduleHeuristic(html);
}

function parseLocalToUTC(localDateISO, timeStr, tz) {
  // Accepts "7:00", "7:00 am", "7:00 p.m.", "19:30"
  const s = (timeStr || '').toLowerCase().replace(/\./g, '').replace(/\s+/g, ' ');
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return null;
  let h = +m[1], mi = +m[2];
  if (/\b(am|pm)\b/.test(s)) {
    const isPM = /\bpm\b/.test(s);
    if (isPM && h < 12) h += 12;
    if (!isPM && h === 12) h = 0;
  }
  return DateTime.fromISO(
    `${localDateISO}T${String(h).padStart(2, '0')}:${String(mi).padStart(2, '0')}:00`,
    { zone: tz }
  ).toUTC().toISO();
}

function materializeDay(rows, localISO, tz) {
  // Turn (start[, stop], title) into UTC spans; infer stop from next start or +60m
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const start = parseLocalToUTC(localISO, r.startLocal, tz);
    if (!start) continue;
    let stop = null;
    if (r.stopLocal) stop = parseLocalToUTC(localISO, r.stopLocal, tz);
    else if (rows[i + 1]?.startLocal) stop = parseLocalToUTC(localISO, rows[i + 1].startLocal, tz);
    if (!stop) stop = DateTime.fromISO(start).plus({ minutes: 60 }).toISO();
    if (DateTime.fromISO(stop) <= DateTime.fromISO(start)) {
      stop = DateTime.fromISO(start).plus({ minutes: 30 }).toISO();
    }
    out.push({ title: r.title, start_ts: start, stop_ts: stop });
  }
  return out;
}

async function fetchChannelDay(url, localISO) {
  // Many GatoTV pages accept /YYYY-MM-DD; if not, base page usually shows “today”
  let u = url;
  if (!/(\/)\d{4}-\d{2}-\d{2}$/.test(url)) u = url.replace(/\/?$/, `/${localISO}`);
  try {
    const html = await fetchHtml(u);
    return { url: u, rows: parseSchedule(html) };
  } catch {
    return { url: u, rows: [] };
  }
}

function clamp24h(programs, nowUTC, hours) {
  const end = nowUTC.plus({ hours });
  return programs
    .filter((p) => DateTime.fromISO(p.start_ts) < end)
    .map((p) => ({
      ...p,
      stop_ts: DateTime.fromISO(p.stop_ts) > end ? end.toISO() : p.stop_ts,
    }));
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
    console.log('No Supabase creds; skip');
    return;
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
    if (error) {
      console.warn(`Programs batch failed: ${error.message}`);
      break;
    }
  }
  console.log(`Program ingest attempted: ${programs.length}`);
}

// ---------- MAIN ----------
async function main() {
  await fs.mkdir('out/mx', { recursive: true });

  // 1) Directory → channel links
  const dirHtml = await fetchHtml(GATOTV_DIR_URL);
  let channels = extractDirectoryChannels(dirHtml);
  if (GATOTV_MAX_CHANNELS > 0 && channels.length > GATOTV_MAX_CHANNELS) {
    channels = channels.slice(0, GATOTV_MAX_CHANNELS);
  }
  await fs.writeFile(
    path.join('out', 'mx', 'gatotv_directory.json'),
    JSON.stringify(channels, null, 2),
    'utf8'
  );
  console.log(`GatoTV directory channels: ${channels.length}`);

  // 2) Today + tomorrow → clamp to 24h window
  const nowUTC = DateTime.utc();
  const localNow = nowUTC.setZone(GATOTV_TZ);
  const todayISO = localNow.toISODate();
  const tomorrowISO = localNow.plus({ days: 1 }).toISODate();

  const programs = [];
  for (const ch of channels) {
    const d1 = await fetchChannelDay(ch.url, todayISO);
    const d2 = await fetchChannelDay(ch.url, tomorrowISO);
    const a1 = materializeDay(d1.rows, todayISO, GATOTV_TZ);
    const a2 = materializeDay(d2.rows, tomorrowISO, GATOTV_TZ);
    const clamped = clamp24h(a1.concat(a2), nowUTC, PROGRAMS_HOURS_AHEAD);

    for (const r of clamped) {
      programs.push({
        // Use the GatoTV channel page URL as the channel_id for now (safe for testing & later SQL joins)
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
        // pack provenance safely into an existing column
        extras: { source: 'gatotv', channel_name: ch.name },
        ingested_at: DateTime.utc().toISO()
      });
    }
  }

  await fs.writeFile(
    path.join('out', 'mx', 'epg_programs_sample.json'),
    JSON.stringify(programs.slice(0, 200), null, 2),
    'utf8'
  );
  await savePrograms(programs);
  console.log(
    `GatoTV 24h ingest complete. Channels scraped: ${channels.length}, programs: ${programs.length}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
