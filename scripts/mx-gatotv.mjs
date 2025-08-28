/* GatoTV full directory crawl (24h) – go down the column on /canales_de_tv,
   open EACH /canal/... link, scrape today's + tomorrow's schedule (24h cap),
   upsert into epg_programs. No XMLTV, no iptv-org matching. */

import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { DateTime } from 'luxon';

const GATOTV_DIR_URL = process.env.GATOTV_DIR_URL || 'https://www.gatotv.com/canales_de_tv';
const GATOTV_TZ = process.env.GATOTV_TZ || 'America/Mexico_City';
const PROGRAMS_HOURS_AHEAD = Number(process.env.PROGRAMS_HOURS_AHEAD || '24');
const GATOTV_MAX_CHANNELS = Number(process.env.GATOTV_MAX_CHANNELS || '0'); // 0 = no cap

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const PROGRAMS_TABLE = process.env.PROGRAMS_TABLE || 'epg_programs';

function cleanText(html) {
  return String(html).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

async function fetchHtml(url) {
  const r = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0' } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return await r.text();
}

function extractDirectoryChannels(html) {
  // Collect every anchor that points to /canal/... (covers /canal/m_0, /canal/_vamos, etc.)
  const out = [];
  const re = /<a\s+[^>]*href=["'](\/canal\/[^"'#?]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const seen = new Set();
  let m;
  while ((m = re.exec(html))) {
    const rel = m[1];
    const name = (m[2] || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const url = new URL(rel, GATOTV_DIR_URL).href;
    if (!name || seen.has(url)) continue;
    seen.add(url);
    out.push({ name, url });
  }
  return out;
}


function parseSchedule(html) {
  // Heuristic extraction of (start[, stop]) + title triples.
  // Works with forms like "7:00 pm - 8:00 pm — <b>Title</b>" or similar markup around the title cell.
  const rows = [];
  const cleaned = html
    .replace(/\r|\n/g, ' ')
    .replace(/<\s*br\s*\/?>(?=\S)/gi, ' ')
    .replace(/\s+/g, ' ');
  const rx = /(\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?)\s*-?\s*(\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?)?[^>]*?<[^>]*?>([^<]{2,200})/gi;
  const seen = new Set();
  let m;
  while ((m = rx.exec(cleaned))) {
    const start = m[1]?.trim();
    const stop = (m[2] || '').trim() || null;
    const title = (m[3] || '').trim();
    const key = `${start}|${stop}|${title}`;
    if (!title || !start || seen.has(key)) continue;
    seen.add(key);
    rows.push({ startLocal: start, stopLocal: stop, title });
  }
  return rows;
}

function parseLocal(dateISO, timeStr, tz) {
  let s = (timeStr || '').toLowerCase().replace(/\./g, '').replace(/\s+/g, ' ');
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return null;
  let h = +m[1],
    mi = +m[2];
  const ampm = /(am|pm)\b/.test(s);
  const isPM = /pm\b/.test(s);
  if (ampm) {
    if (isPM && h < 12) h += 12;
    if (!isPM && h === 12) h = 0;
  }
  return DateTime.fromISO(
    `${dateISO}T${String(h).padStart(2, '0')}:${String(mi).padStart(2, '0')}:00`,
    { zone: tz }
  )
    .toUTC()
    .toISO();
}

function materializeDay(rows, localISO, tz) {
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const start = parseLocal(localISO, r.startLocal, tz);
    if (!start) continue;
    let stop = null;
    if (r.stopLocal) stop = parseLocal(localISO, r.stopLocal, tz);
    else if (rows[i + 1]?.startLocal) stop = parseLocal(localISO, rows[i + 1].startLocal, tz);
    if (!stop) stop = DateTime.fromISO(start).plus({ minutes: 60 }).toISO();
    if (DateTime.fromISO(stop) <= DateTime.fromISO(start))
      stop = DateTime.fromISO(start).plus({ minutes: 30 }).toISO();
    out.push({ title: r.title, start_ts: start, stop_ts: stop });
  }
  return out;
}

async function fetchChannelDay(url, localISO) {
  // Most GatoTV pages accept /YYYY-MM-DD; if not, base page usually shows “today”.
  let u = url;
  if (!/(\/)\d{4}-\d{2}-\d{2}$/.test(url)) u = url.replace(/\/?$/, `/${localISO}`);
  try {
    const html = await fetchHtml(u);
    const rows = parseSchedule(html);
    return { url: u, rows };
  } catch {
    return { url: u, rows: [] };
  }
}

function clamp24h(programs, nowUTC, hours) {
  const end = nowUTC.plus({ hours });
  return programs
    .filter((p) => DateTime.fromISO(p.start_ts) < end)
    .map((p) => ({ ...p, stop_ts: DateTime.fromISO(p.stop_ts) > end ? end.toISO() : p.stop_ts }));
}

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
    let { error } = await client.from(PROGRAMS_TABLE).upsert(slice, {
      onConflict: 'channel_id, start_ts',
    });
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

async function main() {
  await fs.mkdir('out/mx', { recursive: true });

  // 1) Fetch directory and enumerate every /canal/... link (the “first column” list)
  const dirHtml = await fetchHtml(GATOTV_DIR_URL);
  let channels = extractDirectoryChannels(dirHtml);
  if (GATOTV_MAX_CHANNELS > 0 && channels.length > GATOTV_MAX_CHANNELS)
    channels = channels.slice(0, GATOTV_MAX_CHANNELS);
  await fs.writeFile(
    path.join('out', 'mx', 'gatotv_directory.json'),
    JSON.stringify(channels, null, 2),
    'utf8'
  );
  console.log(`GatoTV directory channels found: ${channels.length}`);

  // 2) For each channel page, fetch today + tomorrow and clamp to 24h from now
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
        channel_id: ch.url, // Use GatoTV URL as stable id in this crawl
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
        ingested_at: DateTime.utc().toISO(),
        source_epg: 'gatotv',
        source_url: ch.url,
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
