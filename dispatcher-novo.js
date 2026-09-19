// dispatcher-novo.js – StreamVault Dispatcher (YT) v1
//
// Cópia directa do dispatcher.js v2.5, montada em paralelo no MESMO
// repositório/serviço, pra testar o fluxo yt-dlp sem tocar no dispatcher
// antigo (que continua a servir process-leve.yml/uploader/shard-delete
// normalmente). Reaproveita 100% da estrutura: round-robin de contas,
// DispatchQueue com jitter, retry 429/5xx, auth via GitHub App/PAT,
// jobStore, /webhook, /status, /health.
//
// DIFERENÇAS, TODAS CONSEQUÊNCIA DIRECTA DE CORRER EM PARALELO AO
// DISPATCHER ANTIGO NO MESMO PROCESSO NODE (index.js monta os dois):
//   1. Todas as rotas vivem sob /dlp/* — sem isto colidiriam com as
//      rotas do dispatcher.js antigo (/dispatch, /webhook, etc. na
//      raiz), já que o Express usa a primeira rota que casar.
//   2. Todas as env vars levam sufixo _YT — sem isto, apontar
//      GH_WORKFLOW_FILE pra process-super-leve-dlp.yml quebraria o
//      dispatcher antigo (que também lê GH_WORKFLOW_FILE p'ro
//      process-leve.yml). Podes apontar as contas GH_ACCOUNT_YT_N_* pros
//      MESMOS owner/repo do dispatcher antigo sem problema — cada
//      instância mantém o seu próprio round-robin em memória.
//   3. GET /parent-status/:jobId removido — existia só pro Coordinator
//      de episódios (lotes C/D), que não existe no fluxo yt-dlp (link
//      lógico é sempre 1 watch?v=, sempre 1 job, nunca há "pai com
//      filhos" a rastrear).
//   4. GET /ads-ping removido — já é servido pelo dispatcher antigo na
//      raiz (mesma função, não precisa de 2ª cópia; ficaria inacessível
//      de qualquer forma, o Express já responde pela rota antiga).
//
// Nada de Docker, nada de API nova, nada de infraestrutura adicional —
// yt-dlp corre só dentro do runner do GitHub Actions (substitui o
// libtorrent no process-super-leve-dlp.yml); este ficheiro só despacha.
//
// VARS DE AMBIENTE (mesmo .env do server.js/dispatcher.js):
//   GH_WORKFLOW_FILE_YT     — nome do workflow (default: process-super-leve-dlp.yml)
//   GH_UPLOADER_FILE_YT     — nome do workflow uploader (default: uploader.yml)
//   GH_SHARD_DELETE_FILE_YT — nome do workflow de shard delete (default: shard-delete.yml)
//   GH_WORKFLOW_REF_YT      — branch (default: main)
//   ADMIN_API_KEY_YT        — chave usada pelo Worker novo pra autenticar aqui
//
//   Conta 1 — modo GitHub App (recomendado), pode reaproveitar owner/repo
//   do dispatcher antigo:
//   GH_ACCOUNT_YT_1_APP_ID / _INSTALLATION_ID / _PRIVATE_KEY / _OWNER / _REPO
//   Conta 1 — modo PAT (legado):
//   GH_ACCOUNT_YT_1_TOKEN / _OWNER / _REPO
//   Conta 2: mesmo padrão, trocando "_1_" por "_2_"

import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';

const app = express();
app.use(express.json({ limit: '1mb' }));

// ── CORS — permissivo de propósito. A segurança real deste módulo é o
// x-api-key (verificado dentro de cada rota via requireAccounts/auth),
// não o CORS — CORS é só o browser decidir se deixa o JS ler a resposta,
// nunca impede um cliente não-browser de chamar a rota na mesma. Uma
// whitelist fixa de subdomínios *.workers.dev é frágil (a Cloudflare
// pode atribuir um subdomínio novo a cada deploy sem nome fixo), então
// reflectimos a origem recebida em vez de manter uma lista pra manter
// sincronizada manualmente. ───────────────────────────────────────────
app.use((req, res, next) => {
  const origin = req.headers.origin || '';
  res.setHeader('Access-Control-Allow-Origin', origin || '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const PORT              = parseInt(process.env.PORT || process.env.DISPATCHER_PORT || '3002');
const WORKFLOW_FILE     = process.env.GH_WORKFLOW_FILE_YT     || 'process-super-leve-dlp.yml';
const UPLOADER_FILE     = process.env.GH_UPLOADER_FILE_YT     || 'uploader.yml';
const SHARD_DELETE_FILE = process.env.GH_SHARD_DELETE_FILE_YT || 'shard-delete.yml';
const WORKFLOW_REF      = process.env.GH_WORKFLOW_REF_YT      || 'main';
const ADMIN_KEY         = process.env.ADMIN_API_KEY_YT        || '';

// ── Notificação directa ao worker do lote F ──────────────────────────────────
// FIX: o jobStore abaixo vive só em memória (new Map()) — some inteiro se
// este processo reiniciar/redeployar no Render. O lote-f-worker.js dependia
// só de sondar GET /dlp/status pra saber "terminou", e se o job já não está
// mais no jobStore (por restart), ele nunca vê um status terminal — fica
// preso até o teto de segurança (INFRA_FAILURE_SAFETY_HOURS, horas). Isto
// notifica o worker directamente no momento exacto em que o /dlp/webhook
// chega, sem depender do jobStore sobreviver até lá. Se LOTE_F_WORKER_URL
// não estiver configurada, esta função não faz nada — resto do dispatcher
// funciona normalmente (feature opcional, fail-open).
const LOTE_F_WORKER_URL    = process.env.LOTE_F_WORKER_URL    || '';
const LOTE_F_NOTIFY_SECRET = process.env.LOTE_F_NOTIFY_SECRET || '';

function notifyLoteF(job_id, status) {
  if (!LOTE_F_WORKER_URL) return; // feature desligada, nada a fazer
  const url = `${LOTE_F_WORKER_URL.replace(/\/$/, '')}/internal/notify`;
  // Fire-and-forget — nunca atrasa nem falha a resposta do /dlp/webhook
  // ao GitHub Actions (que já tem o seu próprio timeout/retry).
  fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(LOTE_F_NOTIFY_SECRET ? { 'x-notify-key': LOTE_F_NOTIFY_SECRET } : {}),
    },
    body: JSON.stringify({ job_id, status }),
    signal: AbortSignal.timeout(8000),
  }).then((r) => {
    if (!r.ok) console.warn(`[NOTIFY-LOTE-F] job=${job_id} respondeu HTTP ${r.status}`);
  }).catch((e) => {
    console.warn(`[NOTIFY-LOTE-F] job=${job_id} falhou: ${e.message} (o poll/teto de segurança do lote F continua como rede de segurança)`);
  });
}

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
    const owner = process.env[`GH_ACCOUNT_YT_${n}_OWNER`];
    const repo  = process.env[`GH_ACCOUNT_YT_${n}_REPO`];
    if (!owner || !repo) break;

    const appId          = process.env[`GH_ACCOUNT_YT_${n}_APP_ID`];
    const installationId = process.env[`GH_ACCOUNT_YT_${n}_INSTALLATION_ID`];
    // Permite \n literais no .env (comum ao colar chave PEM numa única linha)
    const privateKeyRaw  = process.env[`GH_ACCOUNT_YT_${n}_PRIVATE_KEY`];
    const privateKey     = privateKeyRaw ? privateKeyRaw.replace(/\\n/g, '\n') : undefined;
    const token          = process.env[`GH_ACCOUNT_YT_${n}_TOKEN`];

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
// CIRÚRGICO: NUNCA process.exit() aqui. Este módulo é importado junto
// com o dispatcher.js antigo e o polygon dentro do MESMO processo Node
// (index.js) — um exit() ao nível do módulo mataria o servidor inteiro
// (incluindo o fluxo antigo, já em produção) só porque as envs GH_
// ACCOUNT_YT_*_OWNER/_REPO ainda não foram configuradas neste serviço.
// Em vez disso, fica sem contas e cada rota que precisa delas responde
// 503 com uma mensagem clara — o resto do processo (dispatcher antigo,
// polygon) continua de pé normalmente.
if (accounts.length === 0) {
  console.error('[dispatcher-novo] AVISO: nenhuma conta GH_ACCOUNT_YT_*_OWNER/_REPO configurada — rotas /dlp/dispatch e /dlp/shard-delete vão responder 503 até isso ser corrigido. O resto do serviço (dispatcher antigo, polygon) não é afectado.');
}

function requireAccounts(res) {
  if (accounts.length > 0) return true;
  res.status(503).json({ error: 'dispatcher-novo sem contas GitHub configuradas (GH_ACCOUNT_YT_1_OWNER/_REPO em falta)' });
  return false;
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
      'User-Agent':           'StreamVault-Dispatcher-YT/1.0',
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
app.post('/dlp/dispatch', auth, async (req, res) => {
  const {
    job_id,
    video_url         = '',
    thumbnail_url     = '',
    seg_duration      = '4',
    max_encode_height = '720',
    metadata          = {},
    timeout_minutes   = '',
    // MISSÃO 1/2 (tipos do process-leve + playlist série/anime): valor
    // a repassar direto pra --playlist-items do yt-dlp dentro do job.
    // Vazio (default) = process-super-leve-dlp.yml decide sozinho
    // (vídeo único pros tipos antigos/movie/documentary/dorama, ou
    // sonda+decide batch pra series, ou playlist inteira pra anime).
    // Preenchido = job já é um batch de série com range definido (veio
    // de um re-enfileiramento no lote F). Puro pass-through — nenhuma
    // lógica de tipo vive aqui, só dentro do próprio .yml.
    playlist_items    = '',
    // Ronda "skip primeiro vídeo": quando 'true', o process-super-
    // leve-dlp.yml ignora sempre o 1º vídeo da playlist na sondagem —
    // ele nunca é baixado/processado, e a numeração/batches do resto já
    // saem sem ele. Puro pass-through, mesma lógica de playlist_items.
    skip_first_item   = 'false',
  } = req.body;

  if (!job_id) return res.status(400).json({ error: 'job_id obrigatório' });
  if (!requireAccounts(res)) return;

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

  // process-super-leve-dlp.yml não tem Coordinator/episódios — os únicos
  // inputs que ele declara em workflow_dispatch são estes 9 (7 de sempre
  // + playlist_items + skip_first_item, adicionados na ronda "tipos +
  // playlist" pra series/anime). Mandar qualquer campo a mais
  // (season_number, parent_job, etc. — herdados do dispatcher.js antigo)
  // faz a API do GitHub responder 422 "Unexpected inputs provided",
  // porque workflow_dispatch valida contra o schema exato declarado no
  // .yml.
  const inputs = {
    job_id,
    video_url,
    thumbnail_url,
    seg_duration:      String(seg_duration),
    max_encode_height: String(max_encode_height),
    metadata: typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
    ...(timeout_minutes ? { timeout_minutes: String(timeout_minutes) } : {}),
    ...(playlist_items ? { playlist_items: String(playlist_items) } : {}),
    ...(skip_first_item ? { skip_first_item: String(skip_first_item) } : {}),
  };

  const isUploader   = isUploaderJob({ ...inputs, metadata });
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
app.post('/dlp/shard-delete', auth, async (req, res) => {
  const { job_id, shard_repo } = req.body;

  if (!job_id || !shard_repo) {
    return res.status(400).json({ error: 'job_id e shard_repo são obrigatórios' });
  }
  if (!requireAccounts(res)) return;

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
// FIX v2.4: batch_total agora pode vir explícito no corpo (caso do
// Coordinator de episódios) — tem prioridade sobre o cálculo antigo via
// metadata.batch_count (mantido só como fallback, usado pelos uploaders
// de sempre, que nunca mandavam esse campo). 'failed' também conta como
// filho "resolvido" na contagem — um episódio que falhou não deve travar
// pra sempre a detecção de "pai concluído" (ver /parent-status abaixo).
app.post('/dlp/webhook', async (req, res) => {
  const { job_id, status, parent_job, batch_total } = req.body;

  if (!job_id) return res.status(400).json({ error: 'job_id obrigatório' });

  const job = jobStore.get(job_id);

  if (!job) {
    console.log(`[WEBHOOK] Job ${job_id} não encontrado no store (uploader/episódio filho?)`);
    return res.status(404).json({ error: 'Job não encontrado', job_id });
  }

  job.status      = status || 'done';
  job.completedAt = new Date().toISOString();
  job.result      = req.body;

  // Avisa o worker do lote F já — não espera o próximo poll dele nem
  // depende deste jobStore continuar vivo até esse poll chegar.
  notifyLoteF(job_id, job.status);

  const account = accounts.find(a => a.id === job.accountId);
  if (account && account.activeJobs > 0) {
    account.activeJobs--;
  }

  if (parent_job && jobStore.has(parent_job)) {
    const parent = jobStore.get(parent_job);
    if (!parent.uploaderResults) parent.uploaderResults = {};
    parent.uploaderResults[job_id] = status || 'done';

    const explicitTotal = Number(batch_total) || 0;
    if (explicitTotal > 0) {
      parent.batchTotal = explicitTotal;
    } else if (!parent.batchTotal) {
      parent.batchTotal = parent.inputs?.metadata
        ? (() => {
            try {
              const m = JSON.parse(parent.inputs.metadata);
              return m.batch_count || 0;
            } catch { return 0; }
          })()
        : 0;
    }

    const totalChildren     = parent.batchTotal || 0;
    const completedChildren = Object.values(parent.uploaderResults)
      .filter(s => s === 'done' || s === 'failed').length;

    console.log(`[WEBHOOK] Filho ${job_id} → ${status} (parent: ${parent_job} ${completedChildren}/${totalChildren || '?'})`);
  }

  console.log(`[WEBHOOK] job=${job_id} status=${status} conta=${account?.owner} active=${account?.activeJobs}`);
  res.json({ ok: true });
});

// ── DELETE /jobs/:jobId — cancelar job ───────────────────────────────────────
app.delete('/dlp/jobs/:jobId', auth, async (req, res) => {
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
app.get('/dlp/status', auth, (_, res) => {
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
app.get('/dlp/health', (_, res) => {
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
console.log(`StreamVault Dispatcher (YT) v1 — montado sob /dlp/*`);
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
