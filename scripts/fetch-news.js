// MPR 뉴스 수집 스크립트 — 네이버 API + 구글 뉴스 RSS → Supabase upsert
// Node 20+ (fetch built-in) 필요
// 필수 환경변수: NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, SUPABASE_URL, SUPABASE_SERVICE_KEY

const fs = require('fs');
const path = require('path');

const {
  NAVER_CLIENT_ID,
  NAVER_CLIENT_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_KEY,
  SERPER_API_KEY, // 선택 — 없으면 구글 웹 검색 스킵
} = process.env;

for (const k of ['NAVER_CLIENT_ID', 'NAVER_CLIENT_SECRET', 'SUPABASE_URL', 'SUPABASE_SERVICE_KEY']) {
  if (!process.env[k]) { console.error(`환경변수 누락: ${k}`); process.exit(1); }
}

// 브랜드별 검색어 (네이버 / 구글 각각)
const BRANDS = {
  'MLB':          { naver: '"MLB" F&F 패션',       google: '"MLB" F&F 패션' },
  'MLB키즈':       { naver: '"MLB키즈"',            google: '"MLB키즈"' },
  '디스커버리':     { naver: '"디스커버리 익스페디션"', google: '"디스커버리 익스페디션"' },
  '디스커버리키즈': { naver: '"디스커버리키즈"',        google: '"디스커버리키즈"' },
  '듀베티카':       { naver: '"듀베티카"',            google: '"듀베티카"' },
  '세르지오타키니': { naver: '"세르지오 타키니"',      google: '세르지오타키니 OR "세르지오 타키니"' },
  '수프라':         { naver: '"수프라" F&F',          google: '"수프라" F&F 패션' },
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

const stripHtml = s => (s || '').replace(/<[^>]*>/g, '')
  .replace(/&quot;/g, '"').replace(/&amp;/g, '&')
  .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&apos;/g, "'");

const hostname = u => { try { return new URL(u).hostname.replace(/^www\./, ''); } catch { return null; } };

// ── 네이버 뉴스 API (100건 × 10페이지 = 브랜드당 최대 1000건) ──
async function fetchNaver(brand, query) {
  const rows = [];
  for (let start = 1; start <= 1000; start += 100) {
    const url = `https://openapi.naver.com/v1/search/news.json?query=${encodeURIComponent(query)}&display=100&start=${start}&sort=date`;
    const res = await fetch(url, {
      headers: {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
      },
    });
    if (!res.ok) {
      console.error(`  네이버 ${brand} start=${start}: HTTP ${res.status}`);
      break;
    }
    const data = await res.json();
    const items = data.items || [];
    if (items.length === 0) break;
    for (const it of items) {
      rows.push({
        brand,
        title: stripHtml(it.title),
        description: stripHtml(it.description) || null,
        link: it.link || null,
        original_link: it.originallink || null,
        source: hostname(it.originallink || it.link),
        pub_date: it.pubDate ? new Date(it.pubDate).toISOString() : null,
        source_platform: 'naver',
        search_query: query,
      });
    }
    if (items.length < 100) break; // 마지막 페이지
    await sleep(150);
  }
  return rows;
}

// ── 구글 뉴스 RSS (XML 직접 파싱, 최대 ~100건) ──
function parseRssXml(xml) {
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRe.exec(xml)) !== null) {
    const block = m[1];
    const pick = (tag) => {
      const cdataRe = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]></${tag}>`);
      const cm = block.match(cdataRe);
      if (cm) return cm[1].trim();
      const plainRe = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\/${tag}>`);
      const pm = block.match(plainRe);
      return pm ? pm[1].trim() : '';
    };
    items.push({
      title: pick('title'),
      link: pick('link'),
      description: pick('description'),
      pubDate: pick('pubDate'),
      source: pick('source'),
    });
  }
  return items;
}

// ── Serper.dev (구글 웹/뉴스 검색 프록시, 월 2500 credit 무료) ──
function parseSerperDate(s) {
  if (!s) return null;
  const now = Date.now();
  const m = String(s).match(/(\d+)\s*(분|시간|일|주|min|minute|hour|day|week)s?\s*(전|ago)/i);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  const unitMs = {
    '분': 60_000, 'min': 60_000, 'minute': 60_000,
    '시간': 3_600_000, 'hour': 3_600_000,
    '일': 86_400_000, 'day': 86_400_000,
    '주': 7 * 86_400_000, 'week': 7 * 86_400_000,
  }[m[2].toLowerCase()] || 0;
  if (!unitMs) return null;
  return new Date(now - n * unitMs).toISOString();
}

async function fetchSerper(brand, query, page = 1) {
  if (!SERPER_API_KEY) return [];
  try {
    const res = await fetch('https://google.serper.dev/news', {
      method: 'POST',
      headers: {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        q: query, gl: 'kr', hl: 'ko', num: 10, page, tbs: 'qdr:w',
      }),
    });
    if (!res.ok) {
      console.error(`  serper ${brand} p${page}: HTTP ${res.status} ${await res.text().catch(() => '')}`);
      return [];
    }
    const data = await res.json();
    const news = data.news || [];
    return news.map(it => ({
      brand,
      title: stripHtml(it.title),
      description: stripHtml(it.snippet) || null,
      link: it.link || null,
      original_link: it.link || null,
      source: it.source || hostname(it.link),
      pub_date: parseSerperDate(it.date),
      source_platform: 'serper',
      search_query: query,
    }));
  } catch (e) {
    console.error(`  serper ${brand}: ${e.message}`);
    return [];
  }
}

async function fetchGoogle(brand, query) {
  const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
  try {
    const res = await fetch(rssUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MPRMonitor/1.0)' },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const xml = await res.text();
    const items = parseRssXml(xml);
    return items.map(it => {
      const fullTitle = stripHtml(it.title);
      const m = fullTitle.match(/\s-\s([^-]+)$/);
      const cleanTitle = m ? fullTitle.replace(/\s-\s[^-]+$/, '').trim() : fullTitle;
      const sourceLabel = m ? m[1].trim() : (it.source || '');
      return {
        brand,
        title: cleanTitle,
        description: stripHtml(it.description) || null,
        link: it.link || null,
        original_link: it.link || null,
        source: sourceLabel || hostname(it.link),
        pub_date: it.pubDate ? new Date(it.pubDate).toISOString() : null,
        source_platform: 'google',
        search_query: query,
      };
    });
  } catch (e) {
    console.error(`  구글 ${brand}: ${e.message}`);
    return [];
  }
}

// ── Supabase upsert (중복은 무시) ──
async function upsertRows(rows) {
  if (rows.length === 0) return 0;
  const CHUNK = 200;
  let okTotal = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const batch = rows.slice(i, i + CHUNK);
    // on_conflict로 UNIQUE CONSTRAINT 명시 → PostgREST가 ON CONFLICT DO NOTHING 처리
    const res = await fetch(`${SUPABASE_URL}/rest/v1/articles?on_conflict=brand,dedup_key`, {
      method: 'POST',
      headers: {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates,return=representation',
      },
      body: JSON.stringify(batch),
    });
    if (!res.ok) {
      console.error(`  upsert 실패 ${res.status}: ${(await res.text()).slice(0, 300)}`);
      continue;
    }
    const inserted = await res.json();
    okTotal += Array.isArray(inserted) ? inserted.length : 0;
  }
  return okTotal;
}

// ── 수집 메타 업데이트 ──
async function updateMeta(summary) {
  await fetch(`${SUPABASE_URL}/rest/v1/collection_meta`, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify({
      key: 'last_sync',
      value: summary,
      updated_at: new Date().toISOString(),
    }),
  });
}

// ── 메인 ──
async function main() {
  const t0 = Date.now();
  const summary = { startedAt: new Date().toISOString(), brands: {}, totalCollected: 0, totalInserted: 0 };

  // Serper는 credit 아끼려고 KST 짝수 시간에만 호출 (2시간 간격)
  // 수동 실행(workflow_dispatch)시엔 무조건 호출
  // 월 예상 사용: 7브랜드 × 2 credit × 12회/일 × 30일 = 2520 credit (무료 한도 2500 근처)
  const kstHour = (new Date().getUTCHours() + 9) % 24;
  const isManual = process.env.GITHUB_EVENT_NAME === 'workflow_dispatch';
  const runSerper = !!SERPER_API_KEY && (isManual || kstHour % 2 === 0);
  console.log(`KST ${kstHour}시 (event=${process.env.GITHUB_EVENT_NAME || 'local'}) — Serper 호출: ${runSerper ? 'YES' : 'skip'}`);

  for (const [brand, q] of Object.entries(BRANDS)) {
    const tb = Date.now();
    const naverRows = await fetchNaver(brand, q.naver);
    const googleRows = await fetchGoogle(brand, q.google);
    let serperRows = [];
    if (runSerper) {
      const serperQuery = q.serper || q.google;
      const p1 = await fetchSerper(brand, serperQuery, 1);
      await sleep(200);
      const p2 = await fetchSerper(brand, serperQuery, 2);
      serperRows = [...p1, ...p2];
    }
    const all = [...naverRows, ...googleRows, ...serperRows];
    const ins = await upsertRows(all);
    summary.brands[brand] = {
      collected: all.length,
      naver: naverRows.length,
      google: googleRows.length,
      serper: serperRows.length,
      inserted: ins,
      ms: Date.now() - tb,
    };
    summary.totalCollected += all.length;
    summary.totalInserted += ins;
    console.log(`  ${brand}: 수집 ${all.length}건 (네이버 ${naverRows.length}/구글 ${googleRows.length}/serper ${serperRows.length}), 신규 ${ins}건, ${((Date.now() - tb) / 1000).toFixed(1)}초`);
    await sleep(300);
  }

  summary.finishedAt = new Date().toISOString();
  summary.totalMs = Date.now() - t0;
  await updateMeta(summary);

  console.log('─'.repeat(50));
  console.log(`완료: 수집 ${summary.totalCollected}건, 신규 저장 ${summary.totalInserted}건, ${(summary.totalMs / 1000).toFixed(1)}초`);

  // data/news.json은 호환성을 위해 유지 (mpr-analyzer 자체 대시보드 호환)
  // Supabase에서 최근 7일치 700건만 덤프
  try {
    const since = new Date();
    since.setDate(since.getDate() - 7);
    const url = `${SUPABASE_URL}/rest/v1/articles?select=brand,title,description,link,original_link,pub_date&pub_date=gte.${since.toISOString()}&order=pub_date.desc&limit=5000`;
    const res = await fetch(url, {
      headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` },
    });
    const arr = await res.json();
    const grouped = {};
    for (const r of arr) {
      if (!grouped[r.brand]) grouped[r.brand] = [];
      grouped[r.brand].push({
        title: r.title,
        originallink: r.original_link,
        link: r.link,
        description: r.description,
        pubDate: r.pub_date ? new Date(r.pub_date).toUTCString() : null,
      });
    }
    const kstNow = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Seoul' }).slice(0, 16);
    const payload = { updatedAt: kstNow, brands: grouped };
    const outPath = path.join(__dirname, '..', 'data', 'news.json');
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), 'utf8');
    console.log(`data/news.json 업데이트 완료 (최근 7일 ${arr.length}건)`);
  } catch (e) {
    console.error('JSON 덤프 실패:', e.message);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
