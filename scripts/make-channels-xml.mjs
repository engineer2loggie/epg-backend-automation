// Build channels.xml by joining iptv-org/api channels+guides and filtering by country.
// Usage: node scripts/make-channels-xml.mjs "PR,US,MX"

import fs from "node:fs/promises";
import path from "node:path";

const API_BASE = "https://iptv-org.github.io/api";

const outDir = "build";
const outPath = path.join(outDir, "channels.xml");

const rawCountries = (process.argv[2] || process.env.COUNTRIES || "").trim();
if (!rawCountries) {
  console.error("Missing country list. Pass e.g. 'PR,US' as an argument or set COUNTRIES env.");
  process.exit(1);
}
const wanted = new Set(
  rawCountries
    .split(",")
    .map(s => s.trim().toUpperCase())
    .filter(Boolean)
);

function uniqBy(arr, keyFn) {
  const seen = new Set();
  const res = [];
  for (const it of arr) {
    const k = keyFn(it);
    if (!seen.has(k)) {
      seen.add(k);
      res.push(it);
    }
  }
  return res;
}

function escapeXml(s = "") {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

(async () => {
  console.log(`Fetching channels.json and guides.json…`);
  const [channelsRes, guidesRes] = await Promise.all([
    fetch(`${API_BASE}/channels.json`),
    fetch(`${API_BASE}/guides.json`)
  ]);
  const [channels, guides] = await Promise.all([channelsRes.json(), guidesRes.json()]);

  // Allowed channel ids by country
  const allowedChannelIds = new Set(
    channels
      .filter(c => c.country && wanted.has(String(c.country).toUpperCase()))
      .map(c => c.id)
  );

  // Build <channel> entries from guides for allowed channels
  const entries = guides
    .filter(g => g.channel && allowedChannelIds.has(g.channel))
    .map(g => ({
      xmltv_id: g.channel,
      site: g.site || "",
      site_id: g.site_id || "",
      lang: g.lang || "",
      site_name: g.site_name || "" // as text node
    }))
    // some channels have multiple guides on same site+site_id; dedupe
    .filter(e => e.site && e.site_id);

  const unique = uniqBy(
    entries,
    e => `${e.xmltv_id}|${e.site}|${e.site_id}|${e.lang}`
  );

  console.log(`Countries: ${[...wanted].join(", ")} → ${unique.length} guide entries`);

  await fs.mkdir(outDir, { recursive: true });

  const xml = [
    `<?xml version="1.0" encoding="UTF-8"?>`,
    `<channels>`
  ];

  for (const e of unique) {
    xml.push(
      `  <channel site="${escapeXml(e.site)}" lang="${escapeXml(e.lang)}" xmltv_id="${escapeXml(e.xmltv_id)}" site_id="${escapeXml(e.site_id)}">${escapeXml(e.site_name)}</channel>`
    );
  }

  xml.push(`</channels>`, ``);

  await fs.writeFile(outPath, xml.join("\n"), "utf8");
  console.log(`Wrote ${outPath}`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
