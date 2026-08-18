// proxy.js – StreamVault Release Proxy v1.1
// ─────────────────────────────────────────────────────────────────────────────
// Corre no Render (free tier) atrás do Cloudflare CDN.
// Recebe GET /{jobId}/{...assetPath} → busca à Release do GitHub → devolve.
//
// O Cloudflare cacheia a resposta por 1 ano — MISS só acontece uma vez por
// asset por região. Após o warm todos os pedidos são servidos do cache CDN.
//
// Fluxo:
//   Player → cdn.pixgo.qzz.io/{jobId}/hls/seg.bin
//   → Cloudflare CDN → HIT → serve (0 requests ao proxy)
//   → MISS → proxy (Render) → GitHub Release asset → cacheia 1 ano
//
// Path: /{jobId}/{...subpath}  — o filename é sempre o último segmento.
//   {jobId}/hls/seg00001.bin   → asset "seg00001.bin"  na Release {jobId}
//   {jobId}/master.m3u8        → asset "master.m3u8"
//   {jobId}/thumbs/thumb_0.jpg → asset "thumb_0.jpg"
//
// Variáveis de ambiente (.env ou Render Dashboard):
//   GITHUB_TOKEN   — PAT com scope: repo
//   GITHUB_OWNER   — username da conta storage
//   GITHUB_REPO    — repo de storage
//   ALLOWED_ORIGIN — origem permitida (ex: https://pixgo.qzz.io ou *)
//   PORT           — porta (Render injeta automaticamente)
// ─────────────────────────────────────────────────────────────────────────────

import 'dotenv/config';
import http from 'http';

const PORT           = parseInt(process.env.PORT || '8080');
const GITHUB_TOKEN   = process.env.GITHUB_TOKEN  || '';
const GITHUB_OWNER   = process.env.GITHUB_OWNER  || '';
const GITHUB_REPO    = process.env.GITHUB_REPO   || '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

const MIME = {
  bin:  'application/octet-stream',
  m3u8: 'application/vnd.apple.mpegurl',
  json: 'application/json',
  jpg:  'image/jpeg',
};

const TTL = 31536000; // 1 ano

// ── GitHub API ───────────────────────────────────────────────────────────────
async function ghFetch(url, extraHeaders = {}) {
  const res = await fetch(url, {
    headers: {
      'Authorization':        `Bearer ${GITHUB_TOKEN}`,
      'Accept':               'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent':           'StreamVault-Proxy/1.1',
      ...extraHeaders,
    },
  });
  return res;
}

// ── Cache de Release em memória (evita hit à API por cada asset) ─────────────
// { jobId → { assets: [{name, id}], cachedAt } }
const releaseCache = new Map();
const RELEASE_CACHE_TTL = 60 * 60 * 1000; // 1 hora

async function getReleaseAssets(jobId) {
  const cached = releaseCache.get(jobId);
  if (cached && Date.now() - cached.cachedAt < RELEASE_CACHE_TTL) {
    return cached.assets;
  }

  const res = await ghFetch(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/tags/${jobId}`
  );

  if (!res.ok) return null;

  const release = await res.json();
  const assets  = (release.assets || []).map(a => ({ name: a.name, id: a.id }));

  releaseCache.set(jobId, { assets, cachedAt: Date.now() });
  return assets;
}

// ── Keep-alive — evita que o Render adormeça ─────────────────────────────────
function startKeepAlive(port) {
  const interval = 14 * 60 * 1000;
  const selfUrl  = process.env.RENDER_EXTERNAL_URL
    ? `${process.env.RENDER_EXTERNAL_URL}/health`
    : `http://localhost:${port}/health`;

  setInterval(async () => {
    try {
      await fetch(selfUrl);
      console.log(`[keep-alive] ping → ${new Date().toISOString()}`);
    } catch (e) {
      console.warn(`[keep-alive] erro: ${e.message}`);
    }
  }, interval);

  console.log(`  ✓ Keep-alive activo → ${selfUrl}`);
}

// ── Stream com backpressure ───────────────────────────────────────────────────
// res.write() devolve false quando o buffer interno está cheio (cliente lento).
// Sem drain, o Node acumula tudo em memória — fatal em Render free com vários
// MISSes simultâneos de ficheiros .bin grandes.
async function streamWithBackpressure(readable, res) {
  const reader = readable.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const ok = res.write(value);
      if (!ok) await new Promise(r => res.once('drain', r));
    }
  } finally {
    reader.releaseLock();
  }
  res.end();
}

// ── Servidor HTTP ────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {

  // CORS
  res.setHeader('Access-Control-Allow-Origin',   ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods',  'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, X-Cache');

  if (req.method === 'OPTIONS') {
    res.writeHead(204); res.end(); return;
  }

  // Health check
  const urlPath = req.url.split('?')[0];
  if (urlPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, repo: `${GITHUB_OWNER}/${GITHUB_REPO}` }));
    return;
  }

  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405); res.end('Method Not Allowed'); return;
  }

  // Parsear path: /{jobId}/{...subpath}
  // O filename é sempre o último segmento — os assets na Release são flat.
  // Exemplos:
  //   /abc123/hls/seg00001.bin   → jobId=abc123  assetName=seg00001.bin
  //   /abc123/master.m3u8        → jobId=abc123  assetName=master.m3u8
  //   /abc123/thumbs/thumb_0.jpg → jobId=abc123  assetName=thumb_0.jpg
  const parts = urlPath.replace(/^\//, '').split('/');
  if (parts.length < 2) {
    res.writeHead(400); res.end('Bad Request'); return;
  }

  const jobId     = parts[0];
  const assetName = parts[parts.length - 1];  // sempre o último segmento
  const ext       = assetName.split('.').pop().toLowerCase();
  const mime      = MIME[ext] || 'application/octet-stream';

  if (!assetName || !ext) {
    res.writeHead(400); res.end('Bad Request'); return;
  }

  if (!GITHUB_TOKEN || !GITHUB_OWNER || !GITHUB_REPO) {
    res.writeHead(500); res.end('Proxy não configurado'); return;
  }

  try {
    // 1. Obter assets da Release (com cache em memória)
    const assets = await getReleaseAssets(jobId);
    if (!assets) {
      res.writeHead(404); res.end(`Release não encontrada: ${jobId}`); return;
    }

    const asset = assets.find(a => a.name === assetName);
    if (!asset) {
      res.writeHead(404); res.end(`Asset não encontrado: ${assetName} em ${jobId}`); return;
    }

    // 2. Descarregar asset da Release
    const assetRes = await ghFetch(
      `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/assets/${asset.id}`,
      { 'Accept': 'application/octet-stream' }
    );

    if (!assetRes.ok) {
      res.writeHead(502); res.end('Erro ao descarregar asset'); return;
    }

    // 3. Resposta com headers de cache imutável
    res.writeHead(200, {
      'Content-Type':  mime,
      'Cache-Control': `public, max-age=${TTL}, immutable`,
      'X-Cache':       'MISS-PROXY',
    });

    if (req.method === 'HEAD') {
      res.end(); return;
    }

    // 4. Stream com backpressure — não acumula em memória
    await streamWithBackpressure(assetRes.body, res);

    console.log(`[proxy] ${jobId}/${assetName} → 200`);

  } catch (e) {
    console.error(`[proxy] erro: ${e.message}`);
    if (!res.headersSent) {
      res.writeHead(500); res.end('Erro interno');
    }
  }
});

// ── Bootstrap ────────────────────────────────────────────────────────────────
// Quando executado directamente (`node proxy.js`) abre a porta.
// Quando importado pelo index.js apenas exporta o server — index.js faz emit.
console.log(`StreamVault Release Proxy v1.1`);
console.log(`  ✓ Storage: ${GITHUB_OWNER}/${GITHUB_REPO}`);

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`  ✓ Proxy a escutar na porta ${PORT}`);
    startKeepAlive(PORT);
  });
}

export { server };
