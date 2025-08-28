/* GatoTV-only EPG (24h) – seed from DB (working channels) → map to GatoTV → scrape schedule table (Hora Inicio / Hora Fin / Programa) → upsert epg_programs
   Notes:
   - No XMLTV fallback in this script.
   - Default TZ for interpreting local times is America/Mexico_City (override via env GATOTV_TZ).
   - channel_id preference: epg_channel_id → tvg_id → gatotv_url (so MVs can keep working once you map IDs).
*/

import fs from "node:fs/promises";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import { DateTime } from "luxon";

/* ------------ ENV ------------ */
const GATOTV_DIR_URL =
  process.env.GATOTV_DIR_URL || "https://www.gatotv.com/canales_de_tv";
const GATOTV_SCORE_MIN = Number(process.env.GATOTV_SCORE_MIN || "0.80");
const GATOTV_TZ = process.env.GATOTV_TZ || "America/Mexico_City";
const PROGRAMS_HOURS_AHEAD = Number(process.env.PROGRAMS_HOURS_AHEAD || "24");
const GATOTV_MAPPING_URL = (process.env.GATOTV_MAPPING_URL || "").trim();

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || "public";
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || "mx_channels";
const PROGRAMS_TABLE = process.env.PROGRAMS_TABLE || "epg_programs";

/* --------- TEXT NORMALIZATION / MATCHING --------- */
function stripAccents(s) {
  return String(s).normalize("NFD").replace(/\p{Diacritic}+/gu, "");
}
const STOP = new Set([
  "canal",
  "tv",
  "television",
  "hd",
  "sd",
  "mx",
  "mexico",
  "méxico",
  "el",
  "la",
  "los",
  "las",
  "de",
  "del",
  "y",
  "en",
  "the",
  "channel",
]);
function tokensOf(s) {
  if (!s) return [];
  let p = stripAccents(String(s).toLowerCase())
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  return p.split(/\s+/).filter((t) => t && !STOP.has(t));
}
function keyOf(s) {
  return Array.from(new Set(tokensOf(s))).sort().join(" ");
}

// keep bigrams as one token (brand phrases)
const PHRASES = [
  ["las", "estrellas"],
  ["azteca", "uno"],
  ["azteca", "7"],
  ["milenio", "television"],
  ["mvs", "tv"],
  ["canal", "once"],
];
function phraseTokens(arr) {
  const out = [...arr];
  for (const [a, b] of PHRASES) {
    for (let i = 0; i < out.length - 1; i++) {
      if (out[i] === a && out[i + 1] === b) {
        out.splice(i, 2, `${a}_${b}`);
      }
    }
  }
  return out;
}

const BRAND = new Set([
  "azteca",
  "adn",
  "milenio",
  "mvs",
  "estrellas",
  "teleformula",
  "teleritmo",
  "multimedios",
  "fox",
  "once",
]);
const CALLSIGNS = new Set(["xew", "xeipn"]);

function jaccardScore(a, b) {
  const A = new Set(phraseTokens(a));
  const B = new Set(phraseTokens(b));
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  const j = inter / (A.size + B.size - inter || 1);
  let brandHits = 0;
  for (const t of A) if (B.has(t) && (BRAND.has(t) || CALLSIGNS.has(t))) brandHits++;
  return j + Math.min(brandHits, 3) * 0.15; // up to +0.45 boost
}

/* ------------- SUPABASE ------------- */
function sb() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
    db: { schema: SUPABASE_SCHEMA },
  });
}

async function fetchWorkingChannelsFromDB() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) return [];
  const { data, error } = await sb()
    .from(SUPABASE_TABLE)
    .select(
      "channel_name, channel_guess, epg_display_name, epg_channel_id, tvg_id, working"
    )
    .eq("working", true);
  if (error || !data) return [];
  // Deduplicate by best display name we have
  const seen = new Map();
  for (const r of data) {
    const name = r.epg_display_name || r.channel_name || r.channel_guess;
    if (!name) continue;
    if (!seen.has(name))
      seen.set(name, {
        name,
        epg_channel_id: r.epg_channel_id || null,
        tvg_id: r.tvg_id || null,
      });
  }
  return [...seen.values()];
}

/* --------- GATOTV DIRECTORY & MATCHING --------- */
async function loadManualMap() {
  if (!GATOTV_MAPPING_URL) return [];
  try {
    const r = await fetch(GATOTV_MAPPING_URL);
    if (!r.ok) return [];
    return await r.json();
  } catch {
    return [];
  }
}

function extractAnchors(html) {
  // Capture both relative (/canal/...) and absolute links to /canal/...
  const anchors = [];
  const re = /<a\s+([^>]*?)>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(html))) {
    const attr = m[1] || "";
    const text = m[2] || "";
    const hrefMatch = attr.match(/href=["']([^"']+)["']/i);
    const href = hrefMatch ? hrefMatch[1] : "";
    if (!href) continue;
    const abs = href.startsWith("http")
      ? href
      : new URL(href, GATOTV_DIR_URL).href;
    if (!/\/canal\//i.test(abs)) continue;
    const titleMatch = attr.match(/title=["']([^"']+)["']/i);
    const title = titleMatch ? titleMatch[1] : "";
    const cleanText = text
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    anchors.push({ url: abs, text: cleanText, title });
  }
  return anchors;
}

async function fetchGatoDirectory() {
  const r = await fetch(GATOTV_DIR_URL, {
    headers: { "user-agent": "Mozilla/5.0" },
  });
  if (!r.ok) throw new Error(`GatoTV dir HTTP ${r.status}`);
  const html = await r.text();
  const raw = extractAnchors(html);

  const out = [];
  const seen = new Set();
  for (const a of raw) {
    if (seen.has(a.url)) continue;
    seen.add(a.url);
    const tokens = [...tokensOf(a.text), ...tokensOf(a.title)];
    out.push({ url: a.url, name: a.text || a.title, tokens });
  }
  return out;
}

function bestGatoMatch(channelName, dir) {
  const t = tokensOf(channelName);
  let best = null,
    score = 0;
  for (const e of dir) {
    const s = jaccardScore(t, e.tokens);
    if (s > score) {
      best = e;
      score = s;
    }
  }
  // Callsign boosts based on the matched entry content
  if (best) {
    const joined = (best.name + " " + best.url).toLowerCase();
    if (/\bxew\b/.test(joined) && /estrellas/.test(channelName.toLowerCase()))
      score += 0.25;
    if (
      /\bxeipn\b/.test(joined) &&
      /(once|canal\s*11)/i.test(channelName.toLowerCase())
    )
      score += 0.2;
  }
  return { entry: best, score: Math.min(score, 1) };
}

/* ------------- SCHEDULE PARSING ------------- */
function parseSpanishScheduleTable(html) {
  // Prefer tables that contain headers like "Hora Inicio" / "Hora Fin" / "Programa"
  const rows = [];
  const tableRe = /<table[\s\S]*?<\/table>/gi;
  let tm;
  while ((tm = tableRe.exec(html))) {
    const table = tm[0];
    if (!/Hora\s*Inicio/i.test(table) || !/Programa/i.test(table)) continue;

    const trRe = /<tr[\s\S]*?<\/tr>/gi;
    let rm;
    let headerSeen = false;
    while ((rm = trRe.exec(table))) {
      const tr = rm[0];
      // First row with headers
      if (!headerSeen && /<t[hd][^>]*>/i.test(tr) && /Hora\s*Inicio/i.test(tr)) {
        headerSeen = true;
        continue;
      }
      if (!headerSeen) continue;

      const cells = [...tr.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)]
        .map((m) =>
          m[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim()
        )
        .filter(Boolean);
      if (!cells.length) continue;

      const timeLike = (s) => /\b\d{1,2}:\d{2}(?:\s*[ap]m)?/i.test(s);
      const start = cells.find(timeLike);
      if (!start) continue;
      const others = cells.filter((x) => x !== start);

      let stop = null;
      const idxStop = others.findIndex(timeLike);
      if (idxStop >= 0) {
        stop = others[idxStop];
        others.splice(idxStop, 1);
      }
      const title = (others.find((x) => x.length > 1) || "").trim();
      if (title) rows.push({ startLocal: start, stopLocal: stop, title });
    }
  }
  return rows;
}

function parseTimeLocal(dateISO, timeStr, tz) {
  let s = (timeStr || "").toLowerCase().replace(/\./g, "").replace(/\s+/g, " ");
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
  const dt = DateTime.fromISO(
    `${dateISO}T${String(h).padStart(2, "0")}:${String(mi).padStart(
      2,
      "0"
    )}:00`,
    { zone: tz }
  );
  return dt.toUTC().toISO();
}

function materializeDay(rows, localISO, tz) {
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const start = parseTimeLocal(localISO, r.startLocal, tz);
    if (!start) continue;
    let stop = null;
    if (r.stopLocal) stop = parseTimeLocal(localISO, r.stopLocal, tz);
    else if (rows[i + 1]?.startLocal)
      stop = parseTimeLocal(localISO, rows[i + 1].startLocal, tz);
    if (!stop) stop = DateTime.fromISO(start).plus({ minutes: 60 }).toISO();
    if (DateTime.fromISO(stop) <= DateTime.fromISO(start))
      stop = DateTime.fromISO(start).plus({ minutes: 30 }).toISO();
    out.push({ title: r.title, start_ts: start, stop_ts: stop });
  }
  return out;
}

async function fetchChannelDay(url, localISO) {
  let u = url;
  if (!/\/(\d{4}-\d{2}-\d{2})$/.test(url)) u = url.replace(/\/?$/, `/${localISO}`);
  const r = await fetch(u, { headers: { "user-agent": "Mozilla/5.0" } });
  if (!r.ok) return { url: u, rows: [] };
  const html = await r.text();
  const rows = parseSpanishScheduleTable(html);
  return { url: u, rows };
}

function clampTo24h(programs, nowUTC, hoursAhead) {
  const endUTC = nowUTC.plus({ hours: hoursAhead });
  return programs
    .filter((x) => DateTime.fromISO(x.start_ts) < endUTC)
    .map((x) => ({
      ...x,
      stop_ts:
        DateTime.fromISO(x.stop_ts) > endUTC ? endUTC.toISO() : x.stop_ts,
    }));
}

/* ------------- DB WRITE ------------- */
async function savePrograms(programs) {
  if (!programs.length) return;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log("Supabase env missing; skipped programs upload.");
    return;
  }
  const client = sb();
  const BATCH = 500;
  for (let i = 0; i < programs.length; i += BATCH) {
    const slice = programs.slice(i, i + BATCH);
    let { error } = await client
      .from(PROGRAMS_TABLE)
      .upsert(slice, { onConflict: "channel_id, start_ts" });
    if (
      error &&
      /no unique|no exclusion constraint/i.test(error.message || "")
    ) {
      ({ error } = await client.from(PROGRAMS_TABLE).insert(slice));
    }
    if (error) {
      console.warn(`Programs batch failed: ${error.message}`);
      break;
    }
  }
  console.log(`Program ingest attempted for ${programs.length} rows`);
}

/* ------------- MAIN ------------- */
async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function main() {
  await ensureDir("out/mx");

  // 1) Seed from DB (working channels). If none, nothing to do.
  const seed = await fetchWorkingChannelsFromDB();
  console.log(`DB working channels: ${seed.length}`);

  if (!seed.length) {
    console.log("No working channels in DB; aborting GatoTV ingest.");
    return;
  }

  // 2) Build directory + optional manual map
  const [dir, manual] = await Promise.all([
    fetchGatoDirectory(),
    loadManualMap(),
  ]);
  const manualMap = new Map();
  for (const m of manual)
    if (m.channel_name && m.gatotv_url)
      manualMap.set(keyOf(m.channel_name), m.gatotv_url);

  // 3) Match → URL
  const matches = [],
    unmatched = [];
  for (const ch of seed) {
    const name = ch.name;
    const k = keyOf(name);
    let url = manualMap.get(k) || null;
    let score = url ? 1 : 0;

    if (!url) {
      const { entry, score: s } = bestGatoMatch(name, dir);
      if (entry) {
        url = entry.url;
        score = s;
      }
    }

    if (url && score >= GATOTV_SCORE_MIN) {
      matches.push({
        channel_name: name,
        url,
        score,
        epg_channel_id: ch.epg_channel_id || null,
        tvg_id: ch.tvg_id || null,
      });
    } else {
      unmatched.push({
        channel_name: name,
        reason: url ? `below_threshold:${score.toFixed(2)}` : "no_candidate",
      });
    }
  }

  await fs.writeFile(
    path.join("out", "mx", "gatotv_matches.json"),
    JSON.stringify(matches, null, 2),
    "utf8"
  );
  await fs.writeFile(
    path.join("out", "mx", "gatotv_unmatched.json"),
    JSON.stringify(unmatched, null, 2),
    "utf8"
  );

  // 4) Fetch schedules for 24h window (today + tomorrow spill)
  const nowUTC = DateTime.utc();
  const localNow = nowUTC.setZone(GATOTV_TZ);
  const todayISO = localNow.toISODate();
  const tomorrowISO = localNow.plus({ days: 1 }).toISODate();

  const programs = [];
  const endUTC = nowUTC.plus({ hours: PROGRAMS_HOURS_AHEAD });

  for (const m of matches) {
    const d1 = await fetchChannelDay(m.url, todayISO);
    const d2 = await fetchChannelDay(m.url, tomorrowISO);
    const a1 = materializeDay(d1.rows, todayISO, GATOTV_TZ);
    const a2 = materializeDay(d2.rows, tomorrowISO, GATOTV_TZ);

    // Combine, clamp to 24h, and push
    const expanded = a1.concat(a2);
    const clamped = clampTo24h(expanded, nowUTC, PROGRAMS_HOURS_AHEAD);

    for (const r of clamped) {
      const channel_id = m.epg_channel_id || m.tvg_id || m.url; // prefer canonical id if present
      programs.push({
        channel_id,
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
        language: "es",
        orig_language: "es",
        credits: null,
        premiere: false,
        previously_shown: false,
        extras: { source: "gatotv", original_channel_ref: m.url },
        ingested_at: DateTime.utc().toISO(),
        source_epg: "gatotv",
        source_url: m.url,
      });
    }
  }

  await fs.writeFile(
    path.join("out", "mx", "epg_programs_sample.json"),
    JSON.stringify(programs.slice(0, 200), null, 2),
    "utf8"
  );

  // 5) Upsert rows
  await savePrograms(programs);

  console.log(
    `GatoTV 24h ingest complete. Channels matched: ${matches.length}, programs: ${programs.length}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
