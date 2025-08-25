// Parse XMLTV and upsert into Supabase.
// Usage: node scripts/push-to-supabase.mjs build/guide.xml build/channels.xml

import fs from "node:fs/promises";
import { parseStringPromise as parseXml } from "xml2js";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TABLE = process.env.SUPABASE_TABLE || "epg_programs";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

function withColonOffset(off) {
  // +0000 => +00:00 ; -0300 => -03:00
  if (!off || off === "Z") return "Z";
  if (/^[+-]\d{4}$/.test(off)) return `${off.slice(0,3)}:${off.slice(3)}`;
  return off;
}

function parseXmlTvDate(s) {
  // Accept: 20250825170000 +0000 OR 20250825170000+0000 OR 20250825170000
  const m = String(s).match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-]\d{4}|Z))?/);
  if (!m) return null;
  const [_, y, mo, d, hh, mm, ss, off] = m;
  const base = `${y}-${mo}-${d}T${hh}:${mm}:${ss}`;
  const iso = off ? `${base}${withColonOffset(off)}` : `${base}Z`;
  const dt = new Date(iso);
  return isNaN(+dt) ? null : dt.toISOString();
}

function arrify(x) {
  return Array.isArray(x) ? x : x ? [x] : [];
}

function titleOf(prog) {
  // xml2js: <title lang="en">Foo</title> => { title: { _: "Foo", $: { lang: "en" } } }
  const t = prog.title;
  if (!t) return "";
  if (typeof t === "string") return t;
  if (Array.isArray(t)) {
    const first = t[0];
    return typeof first === "string" ? first : (first && first._) || "";
  }
  return t._ || "";
}

async function loadChannelMap(channelsXmlPath) {
  const xml = await fs.readFile(channelsXmlPath, "utf8");
  const obj = await parseXml(xml, { explicitArray: false, explicitRoot: true, trim: true });
  const list = arrify(obj?.channels?.channel);
  const map = new Map();
  for (const c of list) {
    const a = c.$ || {};
    if (a.xmltv_id) {
      map.set(String(a.xmltv_id), {
        site: a.site || null,
        site_id: a.site_id || null
      });
    }
  }
  return map;
}

async function* programmeStream(guideXmlPath) {
  const xml = await fs.readFile(guideXmlPath, "utf8");
  const obj = await parseXml(xml, { explicitArray: false, explicitRoot: true, trim: true });
  const progs = arrify(obj?.tv?.programme);
  for (const p of progs) {
    const channelId = p.$?.channel;
    const startIso = parseXmlTvDate(p.$?.start);
    const stopIso = parseXmlTvDate(p.$?.stop);
    const title = titleOf(p);
    if (!channelId || !startIso || !stopIso || !title) continue;
    yield { channelId, startIso, stopIso, title };
  }
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const { error } = await supabase.from(TABLE).upsert(rows, { onConflict: "channel_id,start_utc,title" });
  if (error) throw error;
}

async function main(guideXmlPath, channelsXmlPath) {
  const chMap = await loadChannelMap(channelsXmlPath);

  const batch = [];
  const BATCH_SIZE = 500;

  for await (const p of programmeStream(guideXmlPath)) {
    const meta = chMap.get(p.channelId) || {};
    batch.push({
      channel_id: p.channelId,
      title: p.title,
      start_utc: p.startIso,
      stop_utc: p.stopIso,
      site: meta.site || null,
      site_id: meta.site_id || null
    });
    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(batch.splice(0, batch.length));
    }
  }
  await upsertBatch(batch);
  console.log("Upsert complete.");
}

const [guidePath, channelsPath] = process.argv.slice(2);
if (!guidePath || !channelsPath) {
  console.error("Usage: node scripts/push-to-supabase.mjs build/guide.xml build/channels.xml");
  process.exit(1);
}
main(guidePath, channelsPath).catch(err => {
  console.error(err);
  process.exit(1);
});
