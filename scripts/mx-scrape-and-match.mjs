// -------- Parse EPG (stream) but only keep channels that intersect scraped tokens
async function parseEpgStreamAndIngest(scrapedTokenUniverse){
  console.log(`Downloading EPG (stream)… ${EPG_GZ_URL}`);
  const res = await fetch(EPG_GZ_URL);
  if(!res.ok || !res.body) throw new Error(`Fetch failed ${res.status} ${EPG_GZ_URL}`);

  const gunzip = createGunzip();
  const src = Readable.fromWeb(res.body);
  const decoder = new TextDecoder('utf-8');
  const parser = new SaxesParser({ xmlns:false });

  const nameMap = new Map();       // keyOf(name) => entry
  const entriesById = new Map();   // id => {id,names:Set,tokenSet:Set,hasProgs}

  const now = new Date();
  const minTs = new Date(now.getTime() - EPG_PAST_HOURS*3600*1000);
  const maxTs = new Date(now.getTime() + EPG_WINDOW_HOURS*3600*1000);

  // ---- channel state
  let curCh = null;                 // { id, namesRaw:[] }
  let inDisplay = false;
  const MAX_NAME_CHARS = 512;       // hard cap per <display-name>
  let dispChunks = [];
  let dispLen = 0;
  let dispTruncated = false;

  // ---- programme state
  let inProg = false;
  let curProg = null;
  let curTag = null;
  let textBuf = '';
  const MAX_TXT = 4000;

  // programme batch
  let progBatch = [];

  function flushText(){
    const t = textBuf.trim();
    textBuf = '';
    return t;
  }
  function addCategory(txt){ if(!txt) return; curProg.categories.push(txt); }
  function capPush(obj,key,txt){
    if(!txt) return;
    if(!obj[key]) obj[key]='';
    const remain = Math.max(0, MAX_TXT - obj[key].length);
    if(remain<=0) return;
    obj[key] += (obj[key] ? ' ' : '') + (txt.length>remain ? txt.slice(0,remain) : txt);
  }

  parser.on('error',(e)=>{ throw e; });

  parser.on('opentag',(tag)=>{
    const nm = String(tag.name).toLowerCase();

    // channel tree
    if(nm==='channel'){
      curCh = { id: tag.attributes?.id ? String(tag.attributes.id) : '', namesRaw: [] };
    } else if(nm==='display-name' && curCh){
      inDisplay = true;
      dispChunks = [];
      dispLen = 0;
      dispTruncated = false;
    }

    // programme tree
    else if(nm==='programme'){
      const cid = String(tag.attributes?.channel || '');
      const start = parseXmltvDate(tag.attributes?.start);
      const stop  = parseXmltvDate(tag.attributes?.stop);
      inProg = true;
      curProg = {
        channel_id: cid,
        start_ts: start,
        stop_ts: stop,
        title: '',
        sub_title: '',
        summary: '',
        categories: [],
        language: null,
        orig_language: null,
        episode_num_xmltv: null,
        season: null,
        episode: null,
        is_new: false,
        previously_shown: false,
        premiere: false,
        rating: null,
        star_rating: null,
        icon_url: null,
        program_url: null,
        credits: { director:[], actor:[], writer:[], producer:[], presenter:[] }
      };
      curTag = null;
    } else if(inProg){
      if(['title','sub-title','desc','category','language','orig-language','episode-num','url','star-rating','credits','director','actor','writer','producer','presenter','rating','value','icon','new','previously-shown','premiere'].includes(nm)){
        curTag = nm;
        if(nm==='rating'){
          curProg._ratingSystem = tag.attributes?.system ? String(tag.attributes.system) : null;
        }
        if(nm==='icon' && tag.attributes?.src){
          curProg.icon_url = String(tag.attributes.src);
          curTag = null;
        }
        if(nm==='new' || nm==='previously-shown' || nm==='premiere'){
          if(nm==='new') curProg.is_new = true;
          if(nm==='previously-shown') curProg.previously_shown = true;
          if(nm==='premiere') curProg.premiere = true;
          curTag = null;
        }
      } else {
        curTag = null;
      }
    }
  });

  parser.on('text',(t)=>{
    // BOUNDED buffering for <display-name>
    if(inDisplay && curCh && t && !dispTruncated){
      let chunk = String(t);
      if(chunk.length > MAX_NAME_CHARS) chunk = chunk.slice(0, MAX_NAME_CHARS);
      const remain = MAX_NAME_CHARS - dispLen;
      if(remain <= 0){ dispTruncated = true; return; }
      if(chunk.length > remain){ chunk = chunk.slice(0, remain); dispTruncated = true; }
      if(chunk){ dispChunks.push(chunk); dispLen += chunk.length; }
      return;
    }

    // programme text (already capped by MAX_TXT)
    if(inProg && curProg && curTag){
      textBuf += t;
      if(textBuf.length > MAX_TXT) textBuf = textBuf.slice(0, MAX_TXT);
    }
  });

  parser.on('closetag',(nameRaw)=>{
    const nm = String(nameRaw).toLowerCase();

    // close display-name
    if(nm==='display-name' && curCh){
      const t = dispChunks.length ? dispChunks.join('') : '';
      const clean = t.trim();
      if(clean) curCh.namesRaw.push(clean);
      inDisplay = false;
      dispChunks = []; dispLen = 0; dispTruncated = false;
    }

    // close channel
    else if(nm==='channel' && curCh){
      // keep only channels whose tokens intersect scraped tokens
      const names = new Set();
      for(const n of curCh.namesRaw) for(const v of expandNameVariants(n)) names.add(v);
      for(const v of expandNameVariants(curCh.id)) names.add(v);

      const tokenSet = new Set();
      let intersects = false;
      for(const nm2 of names){
        for(const tok of tokensOf(nm2)){
          tokenSet.add(tok);
          if(!intersects && scrapedTokenUniverse.has(tok)) intersects = true;
        }
      }
      if(intersects){
        const entry = { id: curCh.id, names: [...names], tokenSet, hasProgs:false };
        entriesById.set(curCh.id, entry);
        for(const n of entry.names){
          const k = keyOf(n);
          if(k && !nameMap.has(k)) nameMap.set(k, entry);
        }
      }
      curCh = null;
      inDisplay = false;
      dispChunks = []; dispLen = 0; dispTruncated = false;
    }

    // programme tree handling
    else if(inProg){
      if(nm==='title'){ capPush(curProg,'title',flushText()); curTag=null; }
      else if(nm==='sub-title'){ capPush(curProg,'sub_title',flushText()); curTag=null; }
      else if(nm==='desc'){ capPush(curProg,'summary',flushText()); curTag=null; }
      else if(nm==='category'){ addCategory(flushText()); curTag=null; }
      else if(nm==='language'){ curProg.language = flushText() || curProg.language; curTag=null; }
      else if(nm==='orig-language'){ curProg.orig_language = flushText() || curProg.orig_language; curTag=null; }
      else if(nm==='episode-num'){
        const val = flushText();
        if(val){ curProg.episode_num_xmltv = curProg.episode_num_xmltv ? (curProg.episode_num_xmltv + '; ' + val) : val; }
        curTag=null;
      }
      else if(nm==='value' && curTag==='value'){
        const val = flushText();
        if(curProg._ratingSystem!=null){
          curProg.rating = { system: curProg._ratingSystem, value: val||null };
        } else {
          curProg.star_rating = val || curProg.star_rating;
        }
        curTag=null;
      }
      else if(nm==='url'){ curProg.program_url = flushText() || curProg.program_url; curTag=null; }
      else if(['director','actor','writer','producer','presenter'].includes(nm)){
        const val = flushText(); if(val) curProg.credits[nm].push(val); curTag=null;
      }
      else if(nm==='programme'){
        inProg=false;

        const entry = entriesById.get(curProg.channel_id);
        if(entry) entry.hasProgs = true;

        const hasChannel = !!entry;
        const hasTitle = !!(curProg.title && curProg.title.trim());
        const withinWindow = (curProg.start_ts && curProg.start_ts>=minTs && curProg.start_ts<=maxTs);

        if(hasChannel && hasTitle && withinWindow){
          const row = {
            channel_id: curProg.channel_id,
            start_ts: curProg.start_ts.toISOString(),
            stop_ts: curProg.stop_ts ? curProg.stop_ts.toISOString() : null,
            title: curProg.title || null,
            sub_title: curProg.sub_title || null,
            summary: curProg.summary || null,
            categories: curProg.categories || [],
            language: curProg.language,
            orig_language: curProg.orig_language,
            season: curProg.season,
            episode: curProg.episode,
            episode_num_xmltv: curProg.episode_num_xmltv,
            is_new: curProg.is_new || false,
            previously_shown: curProg.previously_shown || false,
            premiere: curProg.premiere || false,
            rating: curProg.rating || null,
            star_rating: curProg.star_rating || null,
            icon_url: curProg.icon_url || null,
            program_url: curProg.program_url || null,
            credits: curProg.credits,
            extras: null
          };
          progBatch.push(row);
          if(progBatch.length>=EPG_BATCH_SIZE){
            const batch = progBatch; progBatch=[];
            saveProgrammeBatch(batch).catch(e=>console.warn('epg_programs batch error:', e.message));
          }
        }
        curProg=null; curTag=null;
      }
    }
  });

  await new Promise((resolve,reject)=>{
    src.on('error',reject);
    gunzip.on('error',reject);
    gunzip.on('data',(chunk)=>{
      const text = decoder.decode(chunk,{stream:true});
      if(text) parser.write(text);
    });
    gunzip.on('end',()=>{
      parser.write(decoder.decode(new Uint8Array(),{stream:false}));
      parser.close();
      resolve();
    });
    src.pipe(gunzip);
  });

  // Drop channels without programmes
  for(const [k,entry] of nameMap.entries()){
    const e = entriesById.get(entry.id);
    if(!e || !e.hasProgs) nameMap.delete(k);
  }

  // flush any remaining programme rows
  if(progBatch.length) await saveProgrammeBatch(progBatch);

  const kept = new Set([...nameMap.values()]).size;
  console.log(`EPG channels kept (intersect scraped tokens + has programmes): ${kept}`);
  return { nameMap, entries: [...new Set([...nameMap.values()])] };
}
