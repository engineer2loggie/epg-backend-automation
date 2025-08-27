// Scrape iptv-org MX results, extract & probe .m3u8s,
// parse multiple XMLTV EPGs via streaming SAX parser,
// match channels (exact/anchor/subset/fuzzy),
// write artifacts, and upload:
//   - public.mx_channels (streams)
//   - public.epg_programs (program rows)

import { chromium } from 'playwright';
import { createGunzip } from 'node:zlib';
import { Readable } from 'node:stream';
import fs from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createClient } from '@supabase/supabase-js';
import { SaxesParser } from 'saxes';

// ---------- ENV ----------
const SEARCH_URL = process.env.MX_SEARCH_URL || 'https://iptv-org.github.io/?q=live%20country:MX';
const EPG_URLS = (process.env.MX_EPG_URLS || '').trim().split(/\s+/).filter(Boolean);

const HEADLESS = (process.env.HEADLESS ?? 'true') !== 'false';
const MAX_CHANNELS = Number(process.env.MAX_CHANNELS || '0');
const PER_PAGE_DELAY_MS = Number(process.env.PER_PAGE_DELAY_MS || '150');
const NAV_TIMEOUT_MS = Number(process.env.NAV_TIMEOUT_MS || '30000');
const PROBE_TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || '5000');

const FUZZY_MIN = Number(process.env.FUZZY_MIN || '0.45');
const LOG_UNMATCHED = process.env.LOG_UNMATCHED === '1';

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_SCHEMA = process.env.SUPABASE_SCHEMA || 'public';
const STREAMS_TABLE = process.env.SUPABASE_TABLE || 'mx_channels';
const PROGRAMS_TABLE = process.env.PROGRAMS_TABLE || 'epg_programs';

// ---------- helpers: norm & tokens ----------
function stripAccents(s){ return String(s).normalize('NFD').replace(/\p{Diacritic}+/gu,''); }
function normalizeNumerals(s){
  const map={uno:'1',dos:'2',tres:'3',cuatro:'4',cinco:'5',seis:'6',siete:'7',ocho:'8',nueve:'9',diez:'10',once:'11',doce:'12',trece:'13'};
  return String(s).replace(/\b(uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|trece)\b/gi,m=>map[m.toLowerCase()]);
}
function dropTimeshift(s){
  return String(s)
    .replace(/(?:[-+]\s*\d+\s*(?:h|hora|horas)\b)/ig,'')
    .replace(/\b\d+\s*horas?\b/ig,'')
    .replace(/\(\s*\d+\s*horas?\s*\)/ig,'')
    .replace(/\btime\s*shift\b/ig,'')
    .replace(/\s{2,}/g,' ')
    .trim();
}
function stripLeadingCanal(s){ return String(s).replace(/^\s*canal[\s._-]+/i,''); }
function stripCountryTail(s){ return String(s).replace(/(\.(mx|us)|\s+\(?mx\)?|\s+m[eé]xico|\s+usa|\s+eeuu)\s*$/i,'').trim(); }

const STOP=new Set(['canal','tv','television','hd','sd','mx','mexico','méxico','hora','horas','us','usa','eeuu']);
function tokensOf(s){
  if(!s) return [];
  let p=stripAccents(normalizeNumerals(String(s).toLowerCase()));
  p=dropTimeshift(p);
  p=stripCountryTail(p);
  p=p.replace(/&/g,' and ').replace(/[^a-z0-9]+/g,' ').trim();
  return p.split(/\s+/).filter(t=>t && !STOP.has(t));
}
function keyOf(s){ return Array.from(new Set(tokensOf(s))).sort().join(' '); }
function expandNameVariants(s){
  if(!s) return [];
  const out=new Set();
  const orig=String(s).trim();
  const noCanal=stripLeadingCanal(orig);
  const flat=x=>x.replace(/[._(),]+/g,' ').replace(/\s+/g,' ').trim();
  const noTS=dropTimeshift(noCanal);
  const noTail=stripCountryTail(noTS);
  [orig,noCanal,noTS,noTail,flat(orig),flat(noCanal),flat(noTS),flat(noTail)]
    .forEach(v=>{if(v) out.add(v);});
  return [...out];
}
function containsMexicoTag(s){
  const t=stripAccents(String(s).toLowerCase()).replace(/[^a-z0-9]+/g,' ').trim();
  const parts=t.split(/\s+/);
  return parts.includes('mexico') || parts.includes('mx') || /\.mx\b/i.test(String(s));
}
function uniqBy(arr,keyFn){ const m=new Map(); for(const x of arr){const k=keyFn(x); if(!m.has(k)) m.set(k,x);} return [...m.values()]; }

// ---------- scraping ----------
async function parseAllEpg(urls, sb) {
  const keptEntries = new Map(); // id -> { id, names[], tokenSet:Set }
  const nameMap = new Map();     // keyOf(name) -> entry
  const keptIds = new Set();     // channel ids we keep

  // program batching
  const BATCH_SIZE = 500;
  let batch = [];
  const nowIso = new Date().toISOString();

  async function flushBatch() {
    if (!batch.length) return;
    if (!sb) { batch = []; return; } // no DB creds => skip
    const { error } = await sb.from(PROGRAMS_TABLE).insert(batch);
    if (error) {
      console.warn(`Insert programs failed: ${error.message} (${error.code ?? 'no-code'})`);
    } else {
      console.log(`Inserted ${batch.length} program rows`);
    }
    batch = [];
  }

  function xmlKeepDecision(forceAllMex, id, namesRaw) {
    return forceAllMex || isMexicoEntry(id, namesRaw);
  }

  async function parseOneEpg(url) {
    console.log(`Downloading EPG (stream)… ${url}`);
    const res = await fetch(url);
    if (!res.ok || !res.body) throw new Error(`Fetch failed ${res.status} ${url}`);

    const forceAllMex = /_MX/i.test(url);

    const gunzip = createGunzip();
    const src = Readable.fromWeb(res.body);
    const decoder = new TextDecoder('utf-8');
    const parser = new SaxesParser({ xmlns: false });

    // channel state
    let curCh = null; // { id, namesRaw:[] }
    let inChDisp = false, chDispChunks = [], chDispLen = 0;
    const MAX_NAME_CHARS = 1024, MAX_NAMES_PER_CH = 24, MAX_VARIANTS = 64;

    // program state
    let inProg = false;
    let prog = null;
    let inTitle=false, inSub=false, inDesc=false, inCat=false, inLang=false, inOrigLang=false, inUrl=false, inEpNum=false, inRating=false, inRatingVal=false, inStar=false, inStarVal=false, inCredits=false;
    let currentCreditRole = null;
    let ratingSystem = null;

    parser.on('error', (e) => { throw e; });

    parser.on('opentag', (tag) => {
      const nm = String(tag.name).toLowerCase();

      if (nm === 'channel') {
        curCh = { id: tag.attributes?.id ? String(tag.attributes.id) : '', namesRaw: [] };
        inChDisp = false; chDispChunks = []; chDispLen = 0;

      } else if (nm === 'display-name' && curCh) {
        inChDisp = true; chDispChunks = []; chDispLen = 0;

      } else if (nm === 'programme') {
        const cid = String(tag.attributes?.channel || '');
        const startIso = xmltvToIso(tag.attributes?.start || '');
        const stopIso = xmltvToIso(tag.attributes?.stop || '');
        inProg = true;
        prog = {
          channel_id: cid,
          start_ts: startIso,
          stop_ts: stopIso,
          title: null,
          sub_title: null,
          summary: null,
          categories: [],
          language: null,
          orig_language: null,
          premiere: false,
          previously_shown: false,
          episode: null,
          episode_num_xmltv: null,
          program_url: null,
          rating: null,
          star_rating: null,
          credits: {},
          icon_url: null,
          extras: {},
          ingested_at: nowIso
        };

      } else if (inProg) {
        if (nm === 'title') inTitle = true;
        else if (nm === 'sub-title') inSub = true;
        else if (nm === 'desc') inDesc = true;
        else if (nm === 'category') inCat = true;
        else if (nm === 'language') inLang = true;
        else if (nm === 'orig-language') inOrigLang = true;
        else if (nm === 'url') inUrl = true;
        else if (nm === 'episode-num') { inEpNum = true; prog.extras.episode_num_system = tag.attributes?.system || null; }
        else if (nm === 'rating') { inRating = true; ratingSystem = tag.attributes?.system || null; }
        else if (nm === 'value' && inRating) inRatingVal = true;
        else if (nm === 'star-rating') inStar = true;
        else if (nm === 'value' && inStar) inStarVal = true;
        else if (nm === 'credits') { inCredits = true; }
        else if (inCredits) { currentCreditRole = nm; }
        else if (nm === 'icon' && tag.attributes?.src) { if (!prog.icon_url) prog.icon_url = String(tag.attributes.src); }
      }
    });

    parser.on('text', (t) => {
      if (inChDisp && curCh) {
        let chunk = String(t);
        if (!chunk) return;
        if (chDispLen >= MAX_NAME_CHARS) return;
        const remain = MAX_NAME_CHARS - chDispLen;
        if (chunk.length > remain) chunk = chunk.slice(0, remain);
        chDispChunks.push(chunk);
        chDispLen += chunk.length;
        return;
      }

      if (!inProg || !prog || !t) return;
      const txt = String(t).trim();
      if (!txt) return;

      if (inTitle) prog.title = prog.title ? (prog.title + ' ' + txt) : txt;
      else if (inSub) prog.sub_title = prog.sub_title ? (prog.sub_title + ' ' + txt) : txt;
      else if (inDesc) prog.summary = prog.summary ? (prog.summary + ' ' + txt) : txt;
      else if (inCat) prog.categories.push(txt);
      else if (inLang) prog.language = prog.language || txt;
      else if (inOrigLang) prog.orig_language = prog.orig_language || txt;
      else if (inUrl) prog.program_url = prog.program_url || txt;
      else if (inEpNum) {
        prog.episode_num_xmltv = prog.episode_num_xmltv || txt;
        const parsed = parseXmltvNs(txt);
        if (parsed.season != null) prog.season = parsed.season;
        if (parsed.episode != null) prog.episode = parsed.episode;
      }
      else if (inRatingVal) { prog.rating = { system: ratingSystem, value: txt }; }
      else if (inStarVal) { prog.star_rating = txt; }
      else if (inCredits && currentCreditRole) {
        const role = currentCreditRole;
        if (!prog.credits[role]) prog.credits[role] = [];
        prog.credits[role].push(txt);
      }
    });

    parser.on('closetag', async (nameRaw) => {
      const nm = String(nameRaw).toLowerCase();

      // channel pieces
      if (nm === 'display-name' && curCh) {
        const clean = (chDispChunks.length ? chDispChunks.join('') : '').trim();
        if (clean && curCh.namesRaw.length < MAX_NAMES_PER_CH) curCh.namesRaw.push(clean);
        inChDisp = false; chDispChunks = []; chDispLen = 0;
      } else if (nm === 'channel' && curCh) {
        const id = curCh.id || '';
        const keep = xmlKeepDecision(forceAllMex, id, curCh.namesRaw);
        if (keep && id) {
          const names = new Set();
          for (const n of curCh.namesRaw) for (const v of expandNameVariants(n)) if (v) names.add(v);
          for (const v of expandNameVariants(id)) if (v) names.add(v);
          const limited = [];
          for (const v of names) { limited.push(v); if (limited.length >= MAX_VARIANTS) break; }

          const entry = keptEntries.get(id) || { id, names: [], tokenSet: new Set() };
          entry.names = Array.from(new Set(entry.names.concat(limited))).slice(0, MAX_VARIANTS);
          entry.tokenSet = new Set(); for (const nm2 of entry.names) for (const tok of tokensOf(nm2)) entry.tokenSet.add(tok);
          keptEntries.set(id, entry);
          keptIds.add(id);
          for (const n of entry.names) {
            const k = keyOf(n);
            if (k && !nameMap.has(k)) nameMap.set(k, entry);
          }
        }
        curCh = null; inChDisp = false; chDispChunks = []; chDispLen = 0;
      }

      // program flags
      if (inProg && nm === 'premiere') { if (prog) prog.premiere = true; }
      if (inProg && nm === 'previously-shown') { if (prog) prog.previously_shown = true; }

      // programme end
      if (nm === 'programme' && inProg && prog) {
        if (keptIds.has(prog.channel_id) && prog.start_ts && prog.stop_ts) {
          if (!Array.isArray(prog.categories)) prog.categories = [];
          if (!prog.credits) prog.credits = {};
          batch.push(prog);
          if (batch.length >= BATCH_SIZE) await flushBatch();
        }
        inProg = false; prog = null;
        inTitle=inSub=inDesc=inCat=inLang=inOrigLang=inUrl=inEpNum=inRating=inRatingVal=inStar=inStarVal=inCredits=false;
        currentCreditRole = null; ratingSystem = null;
      }

      // child toggles
      if (nm === 'title') inTitle = false;
      else if (nm === 'sub-title') inSub = false;
      else if (nm === 'desc') inDesc = false;
      else if (nm === 'category') inCat = false;
      else if (nm === 'language') inLang = false;
      else if (nm === 'orig-language') inOrigLang = false;
      else if (nm === 'url') inUrl = false;
      else if (nm === 'episode-num') inEpNum = false;
      else if (nm === 'rating') { inRating = false; ratingSystem = null; }
      else if (nm === 'value' && inRatingVal) inRatingVal = false;
      else if (nm === 'star-rating') inStar = false;
      else if (nm === 'value' && inStarVal) inStarVal = false;
      else if (nm === 'credits') { inCredits = false; currentCreditRole = null; }
      else if (inCredits && nm === currentCreditRole) { currentCreditRole = null; }
    });

    await new Promise((resolve, reject) => {
      src.on('error', reject);
      gunzip.on('error', reject);
      gunzip.on('data', async (chunk) => {
        const text = decoder.decode(chunk, { stream: true });
        if (text) parser.write(text);
        if (batch.length >= BATCH_SIZE) await flushBatch();
      });
      gunzip.on('end', async () => {
        parser.write(decoder.decode(new Uint8Array(), { stream: false }));
        parser.close();
        await flushBatch();
        resolve();
      });
      src.pipe(gunzip);
    });
  }

  for (const u of urls) { await parseOneEpg(u); }
  const kept = new Set([...nameMap.values()]);
  console.log(`EPG channels kept (Mexico-related): ${kept.size}`);
  return { nameMap, entries: [...kept] };
}
}

// ---------- matching ----------
function jaccard(aTokens,bTokens){ const A=new Set(aTokens), B=new Set(bTokens); let inter=0; for(const t of A) if(B.has(t)) inter++; return inter/(A.size+B.size-inter||1); }
function findMatch(channelName,nameKey,nameMap,entries){
  const exact=nameMap.get(nameKey); if(exact) return {entry:exact,score:1,method:'exact'};
  const sTokArr=tokensOf(channelName); const sTok=new Set(sTokArr);
  if(sTok.size===1){ const [only]=[...sTok]; for(const e of entries) if(e.tokenSet && e.tokenSet.has(only)) return {entry:e,score:0.99,method:'anchor'}; }
  let subsetBest=null, subsetBestSize=Infinity;
  for(const e of entries){
    const E=e.tokenSet||new Set(); let allIn=true;
    for(const t of sTok){ if(!E.has(t)){ allIn=false; break; } }
    if(allIn && E.size<subsetBestSize){ subsetBest=e; subsetBestSize=E.size; }
  }
  if(subsetBest) return {entry:subsetBest,score:0.9,method:'subset'};
  let best=null,bestScore=0;
  for(const e of entries) for(const nm of e.names){
    const score=jaccard(sTokArr,tokensOf(nm));
    if(score>bestScore){ bestScore=score; best=e; }
  }
  if(best && bestScore>=FUZZY_MIN) return {entry:best,score:bestScore,method:'fuzzy'};
  return {entry:null,score:0,method:'none'};
}

// ---------- DB ----------
async function saveStreams(rows){
  if(!SUPABASE_URL || !SUPABASE_SERVICE_KEY){ console.log('Supabase env missing; skipped DB upload.'); return; }
  if(!rows.length){ console.log('No stream rows to upload.'); return; }
  const supabase=createClient(SUPABASE_URL,SUPABASE_SERVICE_KEY,{auth:{persistSession:false},db:{schema:SUPABASE_SCHEMA}});
  let { error } = await supabase.from(STREAMS_TABLE).upsert(rows,{ onConflict:'stream_url', ignoreDuplicates:false });
  if(error){
    console.warn(`Upsert streams failed (${error.code??'no-code'}): ${error.message}. Falling back to insert…`);
    ({ error } = await supabase.from(STREAMS_TABLE).insert(rows));
  }
  if(error){
    console.warn(`Insert streams failed: ${error.message} (${error.code??'no-code'})`);
  } else {
    console.log(`Stream rows saved: ${rows.length}`);
  }
}

// ---------- MAIN ----------
async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }

async function main(){
  await ensureDir('out/mx');

  // scrape streams
  const browser=await chromium.launch({headless:HEADLESS});
  let filtered=[];
  try{
    console.log(`Scraping: ${SEARCH_URL}`);
    const links=await collectChannelPages(browser);
    console.log(`Found ${links.length} channel pages.`);
    const scraped=await scrapeAll(browser,links);
    console.log(`Channels with at least one .m3u8 (before probe): ${scraped.length}`);
    for(const row of scraped){
      const tested=[];
      for(const s of row.streams){ const ok=await probeM3U8(s.url); if(ok) tested.push(s); }
      row.streams=tested;
    }
    filtered=scraped.filter(r=>r.streams.length>0);
    console.log(`Channels with at least one WORKING .m3u8: ${filtered.length}`);
  } finally {
    await browser.close();
  }

  if(!EPG_URLS.length) throw new Error('No EPG URLs provided in MX_EPG_URLS');

  // parse EPG channels + ingest programs
  const supabase = (SUPABASE_URL && SUPABASE_SERVICE_KEY)
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, { auth:{persistSession:false}, db:{schema:SUPABASE_SCHEMA} })
    : null;

  const { nameMap, entries } = await parseAllEpg(EPG_URLS, supabase);

  // match streams to channels
  const records=[], matchedOnly=[];
  for(const r of filtered){
    const { entry, method } = findMatch(r.channelName, r.channelNameKey, nameMap, entries);
    for(const s of r.streams){
      const rec = {
        stream_url: s.url,
        channel_guess: r.channelName,
        epg_channel_id: entry ? entry.id : null,
        epg_display_name: entry ? (entry.names[0] || null) : null,
        working: true,
        checked_at: new Date().toISOString()
      };
      records.push(rec);
      if(entry) matchedOnly.push({ ...rec, _match_method: method });
    }
  }

  console.log(`Matched with EPG: ${matchedOnly.length} stream rows (across ${filtered.length} channels).`);

  await fs.writeFile(path.join('out','mx','records.json'), JSON.stringify(records,null,2), 'utf8');
  await fs.writeFile(path.join('out','mx','matches.json'), JSON.stringify(matchedOnly,null,2), 'utf8');
  if(LOG_UNMATCHED){
    const matchedUrls = new Set(matchedOnly.map(x=>x.stream_url));
    const unmatched = records.filter(x=>!matchedUrls.has(x.stream_url));
    await fs.writeFile(path.join('out','mx','unmatched.json'), JSON.stringify(unmatched,null,2), 'utf8');
    console.log(`Wrote out/mx/unmatched.json with ${unmatched.length} unmatched rows`);
  }

  await saveStreams(records);

  // small sample dump from programs (already inserted during parse)
  await fs.writeFile(path.join('out','mx','epg_programs_sample.json'),
    JSON.stringify({ sample: 'Programs inserted during parsing; view in DB.' }, null, 2), 'utf8');
}

main().catch((e)=>{ console.error(e); process.exit(1); });
