// scripts/make-channels-by-site.mjs
import fs from 'node:fs/promises';
import path from 'node:path';

const CC = (process.env.CC || 'US').toUpperCase();
const API_DIR = 'work/api';
const OUT_DIR = path.join('work', CC);

// Comma-separated envs -> Set
const blocked = new Set(
  (process.env.BLOCKED_SITES || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
);

// Load JSON helpers
async function loadJson(p) {
  const buf = await fs.readFile(p, 'utf8');
  return JSON.parse(buf);
}

// Minimal XML escape
function esc(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

(async () => {
  await fs.mkdir(OUT_DIR, { recursive: true });

  const channels = await loadJson(path.join(API_DIR, 'channels.json'));
  const guides   = await loadJson(path.join(API_DIR, 'guides.json'));

  // Channels in selected country (id -> channel)
  const byId = new Map(
    channels
      .filter(c => (c.country || '').toUpperCase() === CC)
      .map(c => [c.id, c])
  );

  if (byId.size === 0) {
    console.log(`[${CC}] No channels for this country in channels.json`);
    process.exit(0);
  }

  // Group guides by site for those channel ids
  const grouped = new Map(); // site -> Map(site_id -> xmltv_id)
  for (const g of guides) {
    if (!g || !g.channel || !g.site || !g.site_id) continue;
    if (!byId.has(g.channel)) continue;

    const site = String(g.site).trim();
    if (blocked.has(site)) continue;

    if (!grouped.has(site)) grouped.set(site, new Map());
    // De-dupe by site_id
    grouped.get(site).set(String(g.site_id), String(g.channel));
  }

  if (grouped.size === 0) {
    console.log(`[${CC}] No usable sites (possibly all blocked)`);
    process.exit(0);
  }

  // Write one channels-CC-<site>.xml per site
  for (const [site, map] of grouped) {
    const file = path.join(OUT_DIR, `channels-${CC}-${site}.xml`);
    const lines = ['<channels>'];
    for (const [site_id, xmltv_id] of map) {
      lines.push(
        `  <channel site="${esc(site)}" site_id="${esc(site_id)}" xmltv_id="${esc(xmltv_id)}" />`
      );
    }
    lines.push('</channels>\n');
    await fs.writeFile(file, lines.join('\n'), 'utf8');
    console.log(`[${CC}] wrote ${file} (${map.size} entries)`);
  }
})().catch(err => {
  console.error(err);
  process.exit(1);
});
