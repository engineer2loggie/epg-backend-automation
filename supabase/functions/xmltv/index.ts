// supabase/functions/xmltv/index.ts
// Deno + Supabase Edge Function that emits XMLTV for a country code.
// Query:  GET /xmltv?country=PR
// Optional: GET /xmltv?country=PR&window=12  (hours, defaults to your MV window)

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

type Program = {
  id: string;
  channel_id: string;
  start_time: string;   // ISO UTC
  end_time: string;     // ISO UTC
  title: string | null;
  description: string | null;
};

type Channel = {
  id: string;
  display_name: string | null;
  icon_url: string | null;
};

function asXmlEsc(s: string) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function fmtXmltv(tsIso: string): string {
  // Expecting UTC ISO; return YYYYMMDDHHMMSS +0000
  const d = new Date(tsIso);
  const pad = (n: number, w = 2) => n.toString().padStart(w, "0");
  return (
    d.getUTCFullYear().toString() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds()) +
    " +0000"
  );
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const cc = (url.searchParams.get("country") || "").trim().toUpperCase();
  if (!cc) {
    return new Response("Missing ?country=CC", { status: 400 });
  }

  // (Optional) window param (hours) if you ever want to bypass MV and filter raw table
  const windowHours = parseInt(url.searchParams.get("window") || "12", 10);

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
  const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY")!; // anon is fine for read-only RLS
  const sb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    global: { headers: { Authorization: req.headers.get("Authorization") || "" } },
  });

  // 1) Get programs from the MV (already limited to next 12h by your backend).
  // Country filter: channel_id ending with .cc (e.g., ".pr"). Case-insensitive.
  // NOTE: if you later add a dedicated "country" column, switch to equality.
  const suffix = `.${cc.toLowerCase()}`;
  const { data: programs, error: pErr } = await sb
    .from("programs_next_12h")
    .select("id,channel_id,start_time,end_time,title,description")
    .ilike("channel_id", `%${suffix}`)
    .order("start_time", { ascending: true })
    .limit(200000); // safety cap

  if (pErr) {
    return new Response(JSON.stringify({ error: pErr.message }), { status: 500 });
  }

  // 2) Collect channel IDs and fetch channel metadata
  const channelIds = Array.from(new Set((programs || []).map((p) => p.channel_id)));
  let channels: Channel[] = [];
  if (channelIds.length) {
    // Supabase PostgREST "in" is chunked in client lib, but keep batches small just in case
    const { data: chData, error: cErr } = await sb
      .from("channels")
      .select("id,display_name,icon_url")
      .in("id", channelIds);
    if (cErr) {
      return new Response(JSON.stringify({ error: cErr.message }), { status: 500 });
    }
    channels = chData || [];
  }

  const chMap = new Map<string, Channel>();
  channels.forEach((c) => chMap.set(c.id, c));

  // 3) Build XMLTV
  const now = new Date();
  const header = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE tv SYSTEM "xmltv.dtd">
<tv generator-info-name="supabase-edge-xmltv" generator-info-url="${asXmlEsc(SUPABASE_URL)}" date="${now.toISOString()}">`;

  const channelXml = channelIds
    .map((id) => {
      const c = chMap.get(id);
      const name = asXmlEsc(c?.display_name || id);
      const icon = c?.icon_url ? `  <icon src="${asXmlEsc(c.icon_url)}" />\n` : "";
      return `<channel id="${asXmlEsc(id)}">
  <display-name>${name}</display-name>
${icon}</channel>`;
    })
    .join("\n");

  const programXml = (programs as Program[]).map((p) => {
    // STRICT: only <title> from programs.title; do NOT fall back to <sub-title>
    const title = (p.title || "").trim();
    const desc = (p.description || "").trim();

    const titleXml = title ? `<title>${asXmlEsc(title)}</title>` : `<title />`;
    const descXml = desc ? `<desc>${asXmlEsc(desc)}</desc>` : `<desc />`;

    return `<programme start="${fmtXmltv(p.start_time)}" stop="${fmtXmltv(p.end_time)}" channel="${asXmlEsc(p.channel_id)}">
  ${titleXml}
  ${descXml}
</programme>`;
  }).join("\n");

  const xml = `${header}
${channelXml}
${programXml}
</tv>`;

  return new Response(xml, {
    status: 200,
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      // OkHttp automatically sends Accept-Encoding: gzip; platform will gzip the response.
      "Cache-Control": "max-age=60",
      "Access-Control-Allow-Origin": "*",
    },
  });
});
