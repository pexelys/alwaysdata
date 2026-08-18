// dispatcher.js – StreamVault Dispatcher v2.3
//
// Gere 2 contas GitHub Actions em round-robin.
// Substitui apenas o processQueue/processJob do server.js —
// todas as rotas e lógica do server.js ficam intactas.
//
// FIX v2.3 (jitter + retry na chamada de dispatch ao GitHub):
//   • O sistema de lotes/espaçamento que já existe a montante (server.js
//     enfileirando chamadas a /dispatch) protege a FREQUÊNCIA com que o
//     dispatcher é chamado — mas até agora, cada chamada que chegava ao
//     triggerWorkflow ia direto pra API do GitHub sem nenhum jitter nem
//     retry. Se o server.js mandar uma rajada de /dispatch em sequência
//     (ex: processar uma fila de itens pendentes de uma vez), cada uma
//     virava uma chamada imediata — exatamente o padrão de rajada
//     correlacionada identificado como gatilho de detecção de abuso.
//   • DispatchQueue: fila interna que serializa TODAS as chamadas reais
//     de dispatch ao GitHub (mesmo entre contas diferentes) com um
//     espaçamento mínimo aleatório (jitter) entre elas. Não bloqueia o
//     /dispatch em si — a resposta HTTP só volta depois da chamada
//     real ao GitHub terminar, então bursts de requisições ficam
//     naturalmente espaçados sem precisar de nenhuma mudança no
//     server.js.
//   • Retry com backoff exponencial + jitter em triggerWorkflow: só re-
//     tenta em 429 (rate limit) e 5xx (erro transitório do GitHub) —
//     nunca em 4xx que não seja 429 (esses são erros de payload/permis-
//     são, retry não ajuda). Máximo 3 tentativas, mesmo padrão adotado
//     no coordinator do process-leve.yml.
//
// FIX v2.2 (autenticação via GitHub App):
//   • Cada conta pode agora autenticar via GitHub App (App ID +
//     Installation ID + chave privada) em vez de PAT pessoal. Isto
//     move a atribuição das chamadas de "conta de usuário fazendo
//     requisições em massa via API" para "integração declarada e de
//     escopo restrito", reduzindo a chance de o padrão de disparo
//     automatizado do dispatcher ser lido como atividade anômala de
//     conta pelo GitHub.
//   • Token de instalação é gerado sob demanda (JWT assinado com a
//     chave privada da App, válido 10min) e trocado por um installation
//     access token (válido 1h) via API do GitHub — cacheado em memória
//     e renovado automaticamente ~5min antes de expirar.
//   • COMPATIBILIDADE: contas sem GH_ACCOUNT_N_APP_ID configurado
//     continuam a usar GH_ACCOUNT_N_TOKEN (PAT) como antes — a migração
//     pode ser feita conta a conta, sem downtime.
//
// (mantém tudo de v2.1: fix do warm_concurrency removido do payload,
//  endpoint /shard-delete, correção de CORS para o worker de ingest)
//
// VARS DE AMBIENTE (.env — mesmo ficheiro do server.js):
//   DISPATCHER_PORT      — porta do dispatcher (default: 3002)
//   GH_WORKFLOW_FILE     — nome do workflow (default: process.yml)
//   GH_UPLOADER_FILE     — nome do workflow uploader (default: uploader.yml)
//   GH_SHARD_DELETE_FILE — nome do workflow de shard delete (default: shard-delete.yml)
//   GH_WORKFLOW_REF      — branch (default: main)
//
//   Conta 1 — modo GitHub App (recomendado):
//   GH_ACCOUNT_1_APP_ID          — App ID (visto no topo da página da App)
//   GH_ACCOUNT_1_INSTALLATION_ID — Installation ID (URL de settings/installations/<id>)
//   GH_ACCOUNT_1_PRIVATE_KEY     — conteúdo do .pem gerado (com \n literais se vier de .env)
//   GH_ACCOUNT_1_OWNER           — username/org onde a App está instalada
//   GH_ACCOUNT_1_REPO            — repo com o process.yml / process-leve.yml
//
//   Conta 1 — modo PAT (legado, ainda suportado):
//   GH_ACCOUNT_1_TOKEN   — PAT (scope: repo, workflow)
//   GH_ACCOUNT_1_OWNER   — username
//   GH_ACCOUNT_1_REPO    — repo com o process.yml
//
//   Conta 2: mesmo padrão, trocando "_1_" por "_2_"

import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';

const app = express();
app.use(express.json({ limit: '1mb' }));

// ── CORS — permite chamadas do painel admin (pages.dev, worker de ingest
// e domínio próprio) ─────────────────────────────────────────────────────
app.use((req, res, next) => {
  const allowed = [
    'https://streamvault-admin.pages.dev',
    'https://pixgo.qzz.io',
    'https://digital.pixgo.qzz.io',
    'https://cold-brook-4c20.sheltonnaem.workers.dev',
  ];
  const origin = req.headers.origin || '';
  if (allowed.includes(origin) || !origin) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const PORT              = parseInt(process.env.PORT || process.env.DISPATCHER_PORT || '3002');
const WORKFLOW_FILE     = process.env.GH_WORKFLOW_FILE     || 'process.yml';
const UPLOADER_FILE     = process.env.GH_UPLOADER_FILE     || 'uploader.yml';
const SHARD_DELETE_FILE = process.env.GH_SHARD_DELETE_FILE || 'shard-delete.yml';
const WORKFLOW_REF      = process.env.GH_WORKFLOW_REF      || 'main';
const ADMIN_KEY         = process.env.ADMIN_API_KEY        || '';

// ── Auth via GitHub App (JWT → installation access token) ───────────────────
// Assina um JWT curto (10min) com a chave privada RS256 da App — usado só
// pra trocar por um installation access token, nunca usado diretamente
// nas chamadas de API.
function buildAppJwt(appId, privateKeyPem) {
  const now = Math.floor(Date.now() / 1000);
  const header  = { alg: 'RS256', typ: 'JWT' };
  const payload = {
    iat: now - 60,       // margem de tolerância de relógio
    exp: now + 9 * 60,   // 9min (teto do GitHub é 10min)
    iss: appId,
  };
  const b64url = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
  const unsigned = `${b64url(header)}.${b64url(payload)}`;
  const signature = crypto.sign('RSA-SHA256', Buffer.from(unsigned), privateKeyPem)
    .toString('base64url');
  return `${unsigned}.${signature}`;
}

// Cache de installation tokens em memória — chave por account.id.
const installationTokenCache = new Map(); // id -> { token, expiresAt }

async function getInstallationToken(account) {
  const cached = installationTokenCache.get(account.id);
  const now = Date.now();
  // Renova ~5min antes de expirar, nunca em cima da hora.
  if (cached && cached.expiresAt - now > 5 * 60 * 1000) {
    return cached.token;
  }

  const jwt = buildAppJwt(account.appId, account.privateKey);
  const r = await fetch(
    `https://api.github.com/app/installations/${account.installationId}/access_tokens`,
    {
      method: 'POST',
      headers: {
        'Authorization':        `Bearer ${jwt}`,
        'Accept':               'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
      },
    }
  );

  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Falha ao gerar installation token (conta ${account.owner}): HTTP ${r.status} ${t.slice(0, 200)}`);
  }

  const data = await r.json();
  const expiresAt = new Date(data.expires_at).getTime();
  installationTokenCache.set(account.id, { token: data.token, expiresAt });
  console.log(`[AUTH] Novo installation token gerado para ${account.owner} (expira ${data.expires_at})`);
  return data.token;
}

// Devolve o token a usar nesta chamada — resolve GitHub App (assíncrono)
// ou PAT (síncrono, devolvido já pronto) de forma transparente.
async function resolveToken(account) {
  if (account.mode === 'app') {
    return getInstallationToken(account);
  }
  return account.token; // modo PAT legado
}

// ── Carregar contas ──────────────────────────────────────────────────────────
// Prioriza modo GitHub App (APP_ID + INSTALLATION_ID + PRIVATE_KEY); cai
// para modo PAT (TOKEN) se a App não estiver configurada para aquela conta.
function loadAccounts() {
  const accounts = [];
  let n = 1;
  while (true) {
    const owner = process.env[`GH_ACCOUNT_${n}_OWNER`];
    const repo  = process.env[`GH_ACCOUNT_${n}_REPO`];
    if (!owner || !repo) break;

    const appId          = process.env[`GH_ACCOUNT_${n}_APP_ID`];
    const installationId = process.env[`GH_ACCOUNT_${n}_INSTALLATION_ID`];
    // Permite \n literais no .env (comum ao colar chave PEM numa única linha)
    const privateKeyRaw  = process.env[`GH_ACCOUNT_${n}_PRIVATE_KEY`];
    const privateKey     = privateKeyRaw ? privateKeyRaw.replace(/\\n/g, '\n') : undefined;
    const token          = process.env[`GH_ACCOUNT_${n}_TOKEN`];

    if (appId && installationId && privateKey) {
      accounts.push({
        id: n, owner, repo, activeJobs: 0, lastUsed: null,
        mode: 'app', appId, installationId, privateKey,
      });
      console.log(`  Conta ${n} (${owner}): autenticação via GitHub App`);
    } else if (token) {
      accounts.push({
        id: n, owner, repo, activeJobs: 0, lastUsed: null,
        mode: 'pat', token,
      });
      console.log(`  Conta ${n} (${owner}): autenticação via PAT (legado — considere migrar para GitHub App)`);
    } else {
      console.error(`ERRO: Conta ${n} (${owner}) sem credenciais válidas (nem App, nem PAT) — ignorada.`);
      break;
    }
    n++;
  }
  return accounts;
}

const accounts = loadAccounts();
if (accounts.length === 0) {
  console.error('ERRO: Nenhuma conta GitHub configurada.');
  process.exit(1);
}

// ── Round-robin — conta com menos jobs activos, desempate por lastUsed ───────
function selectAccount() {
  return [...accounts].sort((a, b) => {
    if (a.activeJobs !== b.activeJobs) return a.activeJobs - b.activeJobs;
    const at = a.lastUsed ? new Date(a.lastUsed).getTime() : 0;
    const bt = b.lastUsed ? new Date(b.lastUsed).getTime() : 0;
    return at - bt;
  })[0];
}

// ── GitHub API ───────────────────────────────────────────────────────────────
async function ghFetch(url, account, opts = {}) {
  const token = await resolveToken(account);
  return fetch(url, {
    ...opts,
    headers: {
      'Authorization':        `Bearer ${token}`,
      'Accept':               'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent':           'StreamVault-Dispatcher/2.2',
      ...(opts.headers || {}),
    },
  });
}

/**
 * Detecta se o job é um uploader baseado nos inputs.
 * Uploaders têm: video_url vazio + metadata.is_uploader = true
 * OU file_indices começando com "uploader:"
 */
function isUploaderJob(inputs) {
  try {
    const meta = typeof inputs.metadata === 'string'
      ? JSON.parse(inputs.metadata)
      : inputs.metadata;
    if (meta?.is_uploader === true) return true;
  } catch {}

  if (!inputs.video_url && typeof inputs.file_indices === 'string' && inputs.file_indices.startsWith('uploader:')) {
    return true;
  }

  return false;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── DispatchQueue — serializa chamadas reais de dispatch ao GitHub com
// jitter aleatório entre elas. Complementa (não substitui) o espaçamento
// de lotes que já existe a montante no server.js: mesmo que várias
// chamadas a /dispatch cheguem ao dispatcher em rajada, as chamadas
// efetivas à API do GitHub saem uma de cada vez, com intervalo aleatório.
// Se o espaçamento a montante já for maior que o jitter (caso comum),
// esta fila não introduz atraso extra — só age quando detecta rajada.
class DispatchQueue {
  constructor(minJitterMs, maxJitterMs) {
    this.minJitterMs = minJitterMs;
    this.maxJitterMs = maxJitterMs;
    this.tail = Promise.resolve();
    this.lastRunAt = 0;
  }

  enqueue(task) {
    const run = this.tail.then(async () => {
      const jitter  = this.minJitterMs + Math.random() * (this.maxJitterMs - this.minJitterMs);
      const elapsed = Date.now() - this.lastRunAt;
      if (this.lastRunAt > 0 && elapsed < jitter) {
        await sleep(jitter - elapsed);
      }
      this.lastRunAt = Date.now();
      return task();
    });
    // Garante que uma falha numa tarefa não trava a fila para as seguintes.
    this.tail = run.then(() => {}, () => {});
    return run;
  }
}

// 3-8s de jitter — pequeno o suficiente pra não atrasar percetivelmente
// o /dispatch em uso normal (poucos jobs), mas suficiente pra desfazer
// rajadas de vários disparos simultâneos.
const dispatchQueue = new DispatchQueue(3000, 8000);

function isRetryableStatus(status) {
  return status === 429 || status >= 500;
}

// Executa a chamada real de dispatch, com backoff exponencial + jitter,
// só pra erros transitórios (429/5xx). Erros 4xx (payload inválido,
// permissão, etc.) devolvem na primeira tentativa — retry não ajudaria.
async function triggerWorkflowWithRetry(account, workflowFile, inputs, maxAttempts = 3) {
  let lastResponse = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    lastResponse = await dispatchQueue.enqueue(() => triggerWorkflow(account, workflowFile, inputs));

    if (lastResponse.ok || !isRetryableStatus(lastResponse.status)) {
      return lastResponse;
    }

    if (attempt < maxAttempts) {
      const backoff = Math.pow(2, attempt) * 1000 + Math.random() * 1000; // ~2-3s, ~4-5s
      console.warn(`[DISPATCH] job=${inputs.job_id} HTTP ${lastResponse.status} (tentativa ${attempt}/${maxAttempts}) — retry em ${Math.round(backoff)}ms`);
      await sleep(backoff);
    }
  }
  return lastResponse;
}

async function triggerWorkflow(account, workflowFile, inputs) {
  console.log(`[DISPATCH] job=${inputs.job_id} → workflow=${workflowFile} conta=${account.owner}`);

  return ghFetch(
    `https://api.github.com/repos/${account.owner}/${account.repo}/actions/workflows/${workflowFile}/dispatches`,
    account,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ ref: WORKFLOW_REF, inputs }),
    }
  );
}

async function cancelRun(account, runId) {
  return ghFetch(
    `https://api.github.com/repos/${account.owner}/${account.repo}/actions/runs/${runId}/cancel`,
    account,
    { method: 'POST' }
  );
}

// ── Job store ────────────────────────────────────────────────────────────────
const jobStore = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobStore) {
    const dispatchedAt = job.dispatchedAt ? new Date(job.dispatchedAt).getTime() : 0;
    if (now - dispatchedAt > 24 * 60 * 60 * 1000) {
      jobStore.delete(id);
      console.log(`[CLEANUP] Job expirado removido: ${id}`);
    }
  }
}, 60 * 60 * 1000);

// ── Auth middleware ──────────────────────────────────────────────────────────
function auth(req, res, next) {
  if (!ADMIN_KEY) return next();
  const key = req.headers['x-api-key'] || req.query.key;
  if (key !== ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── POST /dispatch ───────────────────────────────────────────────────────────
app.post('/dispatch', auth, async (req, res) => {
  const {
    job_id,
    video_url         = '',
    thumbnail_url     = '',
    seg_duration      = '4',
    max_encode_height = '720',
    metadata          = {},
    season_number     = '0',
    episode_number    = '0',
    episode_title     = '',
    file_indices      = '',
  } = req.body;

  if (!job_id) return res.status(400).json({ error: 'job_id obrigatório' });

  if (jobStore.has(job_id)) {
    const existing = jobStore.get(job_id);
    const isActive = existing.status === 'dispatched' || existing.status === 'running';
    if (isActive) {
      return res.status(409).json({
        error: 'Job já existe e está ativo',
        job_id,
        status: existing.status,
        dispatchedAt: existing.dispatchedAt,
      });
    }
    console.log(`[DISPATCH] Job ${job_id} re-disparado (status anterior: ${existing.status})`);
    jobStore.delete(job_id);
  }

  const account = selectAccount();

  const inputs = {
    job_id,
    video_url,
    thumbnail_url,
    seg_duration:      String(seg_duration),
    max_encode_height: String(max_encode_height),
    metadata: typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
    season_number:     String(season_number),
    episode_number:    String(episode_number),
    episode_title,
    file_indices,
  };

  const isUploader   = isUploaderJob(inputs);
  const workflowFile = isUploader ? UPLOADER_FILE : WORKFLOW_FILE;

  try {
    const r = await triggerWorkflowWithRetry(account, workflowFile, inputs);

    if (!r.ok) {
      const t = await r.text();
      console.error(`[DISPATCH] GitHub API error ${r.status}: ${t.slice(0, 300)}`);
      return res.status(502).json({
        error: `GitHub dispatch falhou (${r.status})`,
        details: t.slice(0, 300),
      });
    }

    account.activeJobs++;
    account.lastUsed = new Date().toISOString();

    jobStore.set(job_id, {
      jobId:        job_id,
      accountId:    account.id,
      accountOwner: account.owner,
      status:       'dispatched',
      dispatchedAt: new Date().toISOString(),
      isUploader,
      inputs,
    });

    console.log(`[DISPATCH] ✓ job=${job_id} → ${account.owner}/${account.repo} (active=${account.activeJobs}) uploader=${isUploader} thumb=${thumbnail_url ? '✓' : '—'}`);

    res.json({
      ok: true,
      job_id,
      account: account.owner,
      account_id: account.id,
      is_uploader: isUploader,
      workflow: workflowFile,
    });

  } catch (e) {
    console.error(`[DISPATCH] Erro: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /shard-delete ────────────────────────────────────────────────────────
app.post('/shard-delete', auth, async (req, res) => {
  const { job_id, shard_repo } = req.body;

  if (!job_id || !shard_repo) {
    return res.status(400).json({ error: 'job_id e shard_repo são obrigatórios' });
  }

  const account = selectAccount();
  const inputs  = { job_id, shard_repo };

  try {
    const r = await triggerWorkflowWithRetry(account, SHARD_DELETE_FILE, inputs);

    if (!r.ok) {
      const t = await r.text();
      console.error(`[SHARD-DELETE] GitHub API error ${r.status}: ${t.slice(0, 300)}`);
      return res.status(502).json({
        error: `GitHub dispatch falhou (${r.status})`,
        details: t.slice(0, 300),
      });
    }

    console.log(`[SHARD-DELETE] ✓ job_id=${job_id} shard=${shard_repo} → conta=${account.owner}`);
    res.json({ ok: true, job_id, shard_repo, account: account.owner });

  } catch (e) {
    console.error(`[SHARD-DELETE] Erro: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /webhook — callback do Actions quando job termina ───────────────────
app.post('/webhook', async (req, res) => {
  const { job_id, status, parent_job } = req.body;

  if (!job_id) return res.status(400).json({ error: 'job_id obrigatório' });

  const job = jobStore.get(job_id);

  if (!job) {
    console.log(`[WEBHOOK] Job ${job_id} não encontrado no store (uploader filho?)`);
    return res.status(404).json({ error: 'Job não encontrado', job_id });
  }

  job.status      = status || 'done';
  job.completedAt = new Date().toISOString();
  job.result      = req.body;

  const account = accounts.find(a => a.id === job.accountId);
  if (account && account.activeJobs > 0) {
    account.activeJobs--;
  }

  if (parent_job && jobStore.has(parent_job)) {
    const parent = jobStore.get(parent_job);
    if (!parent.uploaderResults) parent.uploaderResults = {};
    parent.uploaderResults[job_id] = status || 'done';

    const totalUploaders = parent.inputs?.metadata
      ? (() => {
          try {
            const m = JSON.parse(parent.inputs.metadata);
            return m.batch_count || 0;
          } catch { return 0; }
        })()
      : 0;

    const completedUploaders = Object.values(parent.uploaderResults).filter(s => s === 'done').length;

    console.log(`[WEBHOOK] Uploader ${job_id} → ${status} (parent: ${parent_job} ${completedUploaders}/${totalUploaders})`);
  }

  console.log(`[WEBHOOK] job=${job_id} status=${status} conta=${account?.owner} active=${account?.activeJobs}`);
  res.json({ ok: true });
});

// ── DELETE /jobs/:jobId — cancelar job ───────────────────────────────────────
app.delete('/jobs/:jobId', auth, async (req, res) => {
  const job = jobStore.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job não encontrado' });

  const account = accounts.find(a => a.id === job.accountId);
  if (account && job.runId) {
    try {
      await cancelRun(account, job.runId);
      console.log(`[CANCEL] Run ${job.runId} cancelado`);
    } catch (e) {
      console.warn(`[CANCEL] Erro ao cancelar run: ${e.message}`);
    }
  }

  job.status = 'cancelled';
  if (account && account.activeJobs > 0) account.activeJobs--;
  res.json({ ok: true, job_id: req.params.jobId });
});

// ── GET /status ──────────────────────────────────────────────────────────────
app.get('/status', auth, (_, res) => {
  const jobs = [...jobStore.values()].map(j => ({
    jobId: j.jobId,
    status: j.status,
    accountOwner: j.accountOwner,
    isUploader: j.isUploader || false,
    dispatchedAt: j.dispatchedAt,
    completedAt: j.completedAt || null,
    uploaderResults: j.uploaderResults || null,
  }));

  res.json({
    accounts: accounts.map(a => ({
      id: a.id,
      owner: a.owner,
      repo: a.repo,
      auth_mode: a.mode,
      activeJobs: a.activeJobs,
      lastUsed: a.lastUsed,
    })),
    jobs,
    total_active: accounts.reduce((s, a) => s + a.activeJobs, 0),
    total_jobs: jobStore.size,
    workflows: {
      process: WORKFLOW_FILE,
      uploader: UPLOADER_FILE,
      shard_delete: SHARD_DELETE_FILE,
      ref: WORKFLOW_REF,
    },
  });
});

// ── GET /health ──────────────────────────────────────────────────────────────
app.get('/health', (_, res) => {
  res.json({
    ok: true,
    accounts: accounts.length,
    active_jobs: accounts.reduce((s, a) => s + a.activeJobs, 0),
    workflow: `${WORKFLOW_FILE}@${WORKFLOW_REF}`,
    uploader: `${UPLOADER_FILE}@${WORKFLOW_REF}`,
    shard_delete: `${SHARD_DELETE_FILE}@${WORKFLOW_REF}`,
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

// ── GET /ads-ping ─────────────────────────────────────────────────────────────
// Sonda de rede usada pelo AdblockGuard (frontend web) — antes batia em
// GET /api/ads/status EdgeOne, que tem limite de requests; aqui não tem
// (Render só limita banda, e isto é só um GIF de 42 bytes). Sem lógica
// nenhuma, sem auth, sem tocar em nada do EdgeOne — só confirma que a
// rede/DNS até este domínio está a passar. CORS aberto de propósito (rota
// pública, chamada direto do browser de qualquer utilizador do site).
app.get('/ads-ping', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', req.headers.origin || '*');
  res.setHeader('Cache-Control', 'no-store, no-cache');
  const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');
  res.setHeader('Content-Type', 'image/gif');
  res.end(pixel);
});

// ── Keep-alive — evita que o Render (free tier) adormeça ────────────────────
function startKeepAlive(port) {
  const interval  = 14 * 60 * 1000; // 14 minutos
  const selfUrl   = process.env.RENDER_EXTERNAL_URL
    ? `${process.env.RENDER_EXTERNAL_URL}/health`
    : `http://localhost:${port}/health`;
  const pipelineUrl = process.env.PIPELINE_API
    ? `${process.env.PIPELINE_API.replace(/\/$/, '')}/health`
    : null;

  setInterval(async () => {
    try {
      const r = await fetch(selfUrl, { signal: AbortSignal.timeout(10000) });
      console.log(`[keep-alive] dispatcher → ${r.status} (${new Date().toISOString()})`);
    } catch (e) {
      console.warn(`[keep-alive] dispatcher ping falhou: ${e.message}`);
    }

    if (pipelineUrl) {
      try {
        const r = await fetch(pipelineUrl, { signal: AbortSignal.timeout(10000) });
        console.log(`[keep-alive] pipeline → ${r.status}`);
      } catch (e) {
        console.warn(`[keep-alive] pipeline ping falhou: ${e.message}`);
      }
    }
  }, interval);

  console.log(`  ✓ Keep-alive activo — ping cada 14min → ${selfUrl}`);
  if (pipelineUrl) console.log(`  ✓ Keep-alive pipeline → ${pipelineUrl}`);
}

// ── Start ────────────────────────────────────────────────────────────────────
console.log(`StreamVault Dispatcher v2.3`);
console.log(`  Accounts: ${accounts.length}`);
accounts.forEach(a => console.log(`    ${a.id}: ${a.owner}/${a.repo} (auth: ${a.mode})`));
console.log(`  Workflows:`);
console.log(`    Process:      ${WORKFLOW_FILE}@${WORKFLOW_REF}`);
console.log(`    Uploader:     ${UPLOADER_FILE}@${WORKFLOW_REF}`);
console.log(`    Shard Delete: ${SHARD_DELETE_FILE}@${WORKFLOW_REF}`);

// Export para uso com index.js (servidor unificado)
export { app };

// Start standalone se executado diretamente
if (process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/^.*[\\/]/, ''))) {
  app.listen(PORT, () => {
    console.log(`\n  ✓ Dispatcher running on http://localhost:${PORT}`);
    startKeepAlive(PORT);
  });
}
