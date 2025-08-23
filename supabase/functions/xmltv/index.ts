// supabase/functions/xmltv/index.ts
// Edge Function: XMLTV feed from Supabase (programs + channels)
// Requires Edge Function Secrets set in dashboard:
//   SUPABASE_URL = https://<project-ref>.supabase.co
//   SUPABASE_SERVICE_ROLE_KEY = <service role key>

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
  global: { headers: { "X-Client-Info": "xmltv-edge-fn" } },
});

/** Simple XML escaper */
function x(s: string): string {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

/** Format ISO string to XMLTV "YYYYMMDDHHMMSS +0000" (always UTC) */
function toXmltvTime(isoUtc: string): string {
  const d = new Date(isoUtc);
  const yyyy = d.getUTCFullYear().toString().padStart(4, "0");
  const MM = (d.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = d.getUTCDate().toString().padStart(2, "0");
  const HH = d.getUTCHours().toString().padStart(2, "0");
  const mm = d.getUTCMinutes().toString().padStart(2, "0");
  const ss = d.getUTCSeconds().toString().padStart(2, "0");
  return `${yyyy}${MM}${dd}${HH}${mm}${ss} +0000`;
}

/** Chunk helper */
function chunk<T>(arr: T[], size = 1000): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/** Build OR filter string like: "channel_id.ilike.%.pr,channel_id.ilike.%.us" */
function buildCcOr(ccs: string[]): string {
  return ccs.map((c) => `channel_id.ilike.%.${c}`).join(",");
}

/** Fetch programs in pages (12h default window), filter by country suffix (e.g. ".pr") and require non-empty title */
async function fetchPrograms(
  ccs: string[],
  startIso: string,
  endIso: string,
): Promise<Array<{
  id: string;
  channel_id: string;
  start_time: string;
  end_time: string;
  title: string;
  description: string | null;
}>> {
  const pageSize = 1000;
  const results: any[] = [];
  let from = 0;

  const orFilter = buildCcOr(ccs);

  // Keep paging until we get less than pageSize
  while (true) {
    const q = supabase
      .from("programs")
      .select("id,channel_id,start_time,end_time,title,description", { count: "exact" })
      .gte("end_time", startIso)
      .lte("start_time", endIso)
      .not("title", "is", null)
      .neq("title", "")
      .neq("title", "Title")
      .neq("title", "No Title")
      .or(orFilter)
      .order("start_time", { ascending: true })
      .range(from, from + pageSize - 1);

    const { data, error } = await q;
    if (error) throw error;

    if (!data || data.length === 0) break;
    results.push(...data);

    if (data.length < pageSize) break;
    from += pageSize;
  }

  // secondary sort by channel then Start (stable output)
  results.sort((a, b) => {
    if (a.channel_id === b.channel_id) {
      return a.start_time.localeCompare(b.start_time);
    }
    return a.channel_id.localeCompare(b.channel_id);
  });

  return results;
}

/** Fetch channels by IDs (in chunks) */
async function fetchChannels(ids: string[]): Promise<Map<string, { display_name: string; icon_url: string | null }>> {
  const map = new Map<string, { display_name: string; icon_url: string | null }>();
  for (const part of chunk(ids, 1000)) {
    const { data, error } = await supabase
      .from("channels")
      .select("id,display_name,icon_url")
      .in("id", part);

    if (error) throw error;
    for (const ch of data ?? []) {
      map.set(ch.id, {
        display_name: ch.display_name ?? ch.id,
        icon_url: ch.icon_url ?? null,
      });
    }
  }
  return map;
}

/** Build XMLTV document string */
function buildXmltv(
  channels: Map<string, { display_name: string; icon_url: string | null }>,
  programs: Array<{ id: string; channel_id: string; start_time: string; end_time: string; title: string; description: string | null }>,
): string {
  let xml = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  xml += `<tv generator-info-name="supabase-xmltv" source-info-name="Open-EPG + IPTV-Org">\n`;

  // Channels (only those that appear in programs)
  const used = new Set(programs.map((p) => p.channel_id));
  for (const id of Array.from(used).sort()) {
    const meta = channels.get(id);
    const name = meta?.display_name ?? id;
    xml += `  <channel id="${x(id)}">\n`;
    xml += `    <display-name>${x(name)}</display-name>\n`;
    if (meta?.icon_url) {
      xml += `    <icon src="${x(meta.icon_url)}" />\n`;
    }
    xml += `  </channel>\n`;
  }

  // Programmes
  for (const p of programs) {
    const start = toXmltvTime(p.start_time);
    const stop = toXmltvTime(p.end_time);
    xml += `  <programme start="${start}" stop="${stop}" channel="${x(p.channel_id)}">\n`;
    xml += `    <title>${x(p.title)}</title>\n`; // STRICT: only <title>, no <sub-title> fallback
    if (p.description && p.description.trim() !== "") {
      xml += `    <desc>${x(p.description)}</desc>\n`;
    }
    xml += `  </programme>\n`;
  }

  xml += `</tv>\n`;
  return xml;
}

// CORS helper
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Origin, Content-Type, Accept",
};

serve(async (req) => {
  try {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    const url = new URL(req.url);
    const params = url.searchParams;

    // cc=PR or cc=PR,US  (we normalize and allow UK->GB)
    const rawCc = (params.get("cc") ?? "").trim();
    if (!rawCc) {
      return new Response(
        `Missing required query param 'cc'. Example: /xmltv?cc=PR&window_h=12`,
        { status: 400, headers: { ...corsHeaders, "Content-Type": "text/plain" } },
      );
    }

    const ccs = rawCc
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
      .map((s) => (s === "uk" ? "gb" : s));

    // window_h=12 (default 12, clamp 1..48)
    const windowH = (() => {
      const n = Number(params.get("window_h") ?? "12");
      if (!Number.isFinite(n)) return 12;
      return Math.min(Math.max(Math.trunc(n), 1), 48);
    })();

    // Time window (UTC)
    const now = new Date();
    const end = new Date(now.getTime() + windowH * 3600_000);

    // Query programs
    const programs = await fetchPrograms(ccs, now.toISOString(), end.toISOString());

    // Collect channel metadata
    const channelIds = Array.from(new Set(programs.map((p) => p.channel_id)));
    const chMap = await fetchChannels(channelIds);

    // Build XMLTV
    const xml = buildXmltv(chMap, programs);

    // Respond
    return new Response(xml, {
      status: 200,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/xml; charset=utf-8",
        "Cache-Control": "public, max-age=120", // 2 minutes
      },
    });
  } catch (err) {
    console.error("xmltv error:", err);
    return new Response(
      `xmltv error: ${err?.message ?? String(err)}`,
      { status: 500, headers: { ...corsHeaders, "Content-Type": "text/plain" } },
    );
  }
});
