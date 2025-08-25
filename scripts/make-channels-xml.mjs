// scripts/make-channels-by-site.mjs
import fs from 'node:fs/promises';
import path from 'node:path';

const CC = (process.env.CC || 'US').toUpperCase();
const API_DIR = 'work/api';
const OUT_DIR = path.join('work', CC);

const blocked = new Set(
  (process.env.BLOCKED_SITES || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
);

async function loadJson(p) {
  const buf = await fs.readFile(p, 'utf8');
  return JSON.parse(buf);
}

function esc(s) {
  // avoid replaceAll & template literals for maximum portability
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

(async () => {
  await fs.mkdir(OUT_DIR, { recursive: true });

  const channels = await loadJson(path.join(API_DIR, 'channels.json'));
  const guides   = await loadJson(path.join(API_DIR, 'guides.json'));

  // index channels by id for this country
  const byId = new Map(
    channels
      .filter(c => ((c.country || '').toUpperCase() === CC))
      .map(c => [c.id, c])
  );

  if (byId.size === 0) {
    console.log('[' + CC + '] No channels for this country in channels.json');
    return;
  }

  // site -> Map(site_id -> xmltv_id)
  const grouped = new Map();

  for (const g of guides) {
    if (!g || !g.channel || !g.site || !g.site_id) continue;
    if (!byId.has(g.channel)) continue;

    const site = String(g.site).trim();
    if (blocked.has(site)) continue;

    if (!grouped.has(site)) grouped.set(site, new Map());
    grouped.get(site).set(String(g.site_id), String(g.channel));
  }

  if (grouped.size === 0) {
    console.log('[' + CC + '] No usable sites (possibly all blocked)');
    return;
  }

  for (const [site, map] of grouped) {
    const file = path.join(OUT_DIR, 'channels-' + CC + '-' + site + '.xml');
    const lines = ['<channels>'];
    for (const [site_id, xmltv_id] of map) {
      lines.push(
        '  <channel site="' + esc(site) + '" site_id="' + esc(site_id) + '" xmltv_id="' + esc(xmltv_id) + '" />'
      );
    }
    lines.push('</channels>');
    lines.push(''); // trailing newline
    await fs.writeFile(file, lines.join('\n'), 'utf8');
    console.log('[' + CC + '] wrote ' + file + ' (' + map.size + ' entries)');
  }
})().catch(err => {
  console.error(err);
  process.exit(1);
});
