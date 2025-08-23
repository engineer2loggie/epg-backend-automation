// supabase/functions/xmltv/index.ts
// Deno Edge Function: serve XMLTV from Supabase
// - Window: next 12h by default
// - Country filter via ?cc=PR (comma-separated ok)
// - Title strictly from programs.title
// - Optional gzip via ?gzip=1 or Accept-Encoding:gzip

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ---------- Config ----------
const DEFAULT_WINDOW_HOURS = 12;
const DEFAULT_COUNTRIES = ["PR", "US", "MX", "ES", "DE", "CA", "IT", "GB", "IE", "CO", "AU"];
const USE_MV = false; // set true if you have a materialized view `programs_next_12h`

// ---------- Supabase ----------
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars");
}

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ---------- Helpers ----------
function xmlEscape(s: string | null | undefined): string {
  if (!s) return "";
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function fmtXmltvUTC(iso: string | Date): string {
  const d = typeof iso === "string" ? new Date(iso) : iso;
  // Produce YYYYMMDDHHMMSS +0000
  const pad = (n: number, w = 2) => n.toString().padStart(w, "0");
  const y = d.getUTCFullYear();
  const M = pad(d.getUTCMonth() + 1);
  const D = pad(d.getUTCDate());
  const h = pad(d.getUTCHours());
  const m = pad(d.getUTCMinutes());
  const s = pad(d.getUTCSeconds());
  return `${y}${M}${D}${h}${m}${s} +0000`;
}

function parseCountriesParam(url: URL): string[] {
  const raw = url.searchParams.get("cc");
  const list = (raw ? raw.split(",") : DEFAULT_COUNTRIES).map((c) => c.trim().toLowerCase()).filter(Boolean);
  return Array.from(new Set(list));
}

function wantsGzip(req: Request, url: URL): boolean {
  if (url.searchParams.get("gzip")?.trim() === "1") return true;
  const ae = req.headers.get("accept-encoding") || "";
  return /\bgzip\b/i.test(ae);
}

function buildOrSuffix(ccs: string[]): string {
  // PostgREST .or() filter expects: "channel_id.ilike.%.pr,channel_id.ilike.%.us"
  const terms = ccs.map((c) => `channel_id.ilike.%.${c}`);
  return terms.join(",");
}

type ProgramRow = {
  id: string;
  channel_id: string;
  start_time: string; // ISO UTC
  end_time: string;   // ISO UTC
  title: string | null;
  description: string | null;
};

type ChannelRow = {
  id: string;
  display_name: string | null;
  icon_url: string | null;
};

// ---------- Core ----------
serve(async (req) => {
  try {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    const url = new URL(req.url);

    const windowHours = Math.max(1, Math.min(24, parseInt(url.searchParams.get("hours") || `${DEFAULT_WINDOW_HOURS}`, 10) || DEFAULT_WINDOW_HOURS));
    const now = new Date();
    const end = new Date(now.getTime() + windowHours * 60 * 60 * 1000);

    // Filters
    const countries = parseCountriesParam(url); // ["pr","us",...]
    const orSuffix = buildOrSuffix(countries);

    // Optional: a single channel filter for quick tests (?channel_id=ABC.pr)
    const testChannelId = url.searchParams.get("channel_id") || undefined;

    // Pull programs within time window (UTC) and matching channel id suffix (country code by .xx)
    let programs: ProgramRow[] = [];

    if (USE_MV) {
      // If you created materialized view `programs_next_12h`
      let q = sb.from("programs_next_12h")
        .select("id,channel_id,start_time,end_time,title,description")
        .order("channel_id", { ascending: true })
        .order("start_time", { ascending: true });

      // still re-bound to now..now+windowHours in case MV is slightly stale or you changed hours:
      q = q.gte("end_time", now.toISOString()).lte("start_time", end.toISOString());

      if (countries.length) q = q.or(orSuffix);
      if (testChannelId) q = q.eq("channel_id", testChannelId);

      const { data, error } = await q.limit(200000);
      if (error) throw error;
      programs = data || [];
    } else {
      let q = sb.from("programs")
        .select("id,channel_id,start_time,end_time,title,description")
        .gte("end_time", now.toISOString())
        .lte("start_time", end.toISOString())
        .order("channel_id", { ascending: true })
        .order("start_time", { ascending: true });

      if (countries.length) q = q.or(orSuffix);
      if (testChannelId) q = q.eq("channel_id", testChannelId);

      // Cap to prevent runaway payloads (tune as needed)
      const { data, error } = await q.limit(200000);
      if (error) throw error;
      programs = data || [];
    }

    // Collect channel ids present in programs
    const chIds = Array.from(new Set(programs.map((p) => p.channel_id)));
    let channels: Record<string, ChannelRow> = {};
    if (chIds.length) {
      // Fetch channel rows in one shot
      const { data: chRows, error: chErr } = await sb.from("channels")
        .select("id,display_name,icon_url")
        .in("id", chIds)
        .limit(chIds.length);
      if (chErr) throw chErr;
      for (const ch of chRows || []) channels[ch.id] = ch;
    }

    // Build XMLTV
    let xml = `<?xml version="1.0" encoding="UTF-8"?>\n`;
    xml += `<!DOCTYPE tv SYSTEM "xmltv.dtd">\n`;
    xml += `<tv generator-info-name="iptv4u-edge" source-info-name="open-epg + supabase">\n`;

    // Channels
    for (const chId of chIds) {
      const ch = channels[chId] || { id: chId, display_name: chId, icon_url: null };
      xml += `  <channel id="${xmlEscape(ch.id)}">\n`;
      xml += `    <display-name>${xmlEscape(ch.display_name || ch.id)}</display-name>\n`;
      if (ch.icon_url) {
        xml += `    <icon src="${xmlEscape(ch.icon_url)}"/>\n`;
      }
      xml += `  </channel>\n`;
    }

    // Programmes (title strictly from programs.title; description optional)
    for (const p of programs) {
      const start = fmtXmltvUTC(p.start_time);
      const stop = fmtXmltvUTC(p.end_time);
      xml += `  <programme start="${start}" stop="${stop}" channel="${xmlEscape(p.channel_id)}">\n`;
      if (p.title && p.title.trim() !== "") {
        xml += `    <title>${xmlEscape(p.title)}</title>\n`;
      } else {
        // Leave out <title> altogether when empty (per your requirement: no subtitle fallback)
      }
      if (p.description && p.description.trim() !== "") {
        xml += `    <desc>${xmlEscape(p.description)}</desc>\n`;
      }
      xml += `  </programme>\n`;
    }

    xml += `</tv>\n`;

    // Gzip if requested / supported
    const gzip = wantsGzip(req, url);
    const headers = new Headers({
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=120",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
    });

    if (gzip) {
      headers.set("Content-Encoding", "gzip");
      const stream = new Blob([xml]).stream().pipeThrough(new CompressionStream("gzip"));
      return new Response(stream, { status: 200, headers });
    }

    return new Response(xml, { status: 200, headers });
  } catch (err) {
    const msg = (err && (err as any).message) || String(err);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders(),
      },
    });
  }
});

function corsHeaders(): HeadersInit {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  };
}
