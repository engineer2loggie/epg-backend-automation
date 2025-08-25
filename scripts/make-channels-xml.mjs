// scripts/make-channels-by-site.mjs
// Usage: node scripts/make-channels-by-site.mjs US
// Produces: work/US/channels-US-<site>.xml (one per site) filtered to that country's channels.

import fs from "fs";
import path from "path";
import https from "https";

const CC_INPUT = (process.argv[2] || "").toUpperCase();
if (!CC_INPUT) {
  console.error("Country code required (e.g. US, GB, PR, MX, CA, IT, ES, AU, IE, DE, DO)");
  process.exit(1);
}
const CC = CC_INPUT === "UK" ? "GB" : CC_INPUT;

// Sites that tend to 403 on GitHub runners; can be changed via env
const BLOCKED = new Set(
  (process.env.BLOCKED_SITES || "directv.com,mi.tv,tvtv.us,tvpassport.com,gatotv.com")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

function httpGet(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "User-Agent": "epg-job/1.0" } }, res => {
        if (res.statusCode !== 200) {
          reject(new Error(`GET ${url} -> ${res.statusCode}`));
          res.resume();
          return;
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", chunk => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

function escapeXml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const OUT_DIR = path.join("work", CC);
fs.mkdirSync(OUT_DIR, { recursive: true });

(async () => {
  console.log(`[make-channels] country=${CC}`);

  const channelsUrl = "https://iptv-org.github.io/api/channels.json";
  const guidesUrl   = "https://iptv-org.github.io/api/guides.json";

  const [channelsRaw, guidesRaw] = await Promise.all([
    httpGet(channelsUrl),
    httpGet(guidesUrl)
  ]);

  /** @type {{id:string,country?:string,name?:string,language?:string[]}|[]} */
  const channels = JSON.parse(channelsRaw);
  /** @type {{channel:string|null,site:string,site_id:string,site_name?:string,lang?:string}|[]} */
  const guides = JSON.parse(guidesRaw);

  // Index country channels by id
  const byId = new Map(
    channels
      .filter(ch => (ch.country || "").toUpperCase() === CC)
      .map(ch => [ch.id, ch])
  );

  // Group by site, but only for this country's channels
  const perSite = new Map(); // site -> array of entries
  for (const g of guides) {
    if (!g || !g.channel || !g.site || !g.site_id) continue;
    if (!byId.has(g.channel)) continue;
    if (BLOCKED.has(g.site)) continue;

    const arr = perSite.get(g.site) || [];
    arr.push({
      site: g.site,
      site_id: g.site_id,
      xmltv_id: g.channel,
      lang: g.lang || ""
    });
    perSite.set(g.site, arr);
  }

  // Write one channels-<CC>-<site>.xml per site
  for (const [site, arr] of perSite.entries()) {
    const file = path.join(OUT_DIR, `channels-${CC}-${site}.xml`);
    const rows = arr
      .sort((a, b) => a.xmltv_id.localeCompare(b.xmltv_id))
      .map(
        x =>
          `  <channel site="${x.site}" site_id="${escapeXml(x.site_id)}" xmltv_id="${escapeXml(
            x.xmltv_id
          )}"${x.lang ? ` lang="${x.lang}"` : ""}/>`
      )
      .join("\n");
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<channels>\n${rows}\n</channels>\n`;
    fs.writeFileSync(file, xml, "utf8");
    console.log(`[make-channels] wrote ${file} (${arr.length} entries)`);
  }

  if (perSite.size === 0) {
    const f = path.join(OUT_DIR, "README.txt");
    fs.writeFileSync(f, `No sites produced for ${CC}\n`, "utf8");
    console.log(`[make-channels] ${f}`);
  }
})().catch(err => {
  console.error(err);
  process.exit(1);
});
