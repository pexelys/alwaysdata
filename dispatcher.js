// dispatcher.js — Dispatcher do catálogo (Render).
//
// Papel único: receber do worker Cloudflare um pedido autenticado e disparar o
// workflow de descoberta no GitHub Actions. É o único sítio que guarda as
// credenciais GitHub (GH_ACCOUNT_*), por isso o worker não precisa de nenhum
// secret GitHub. Sem estado: o estado dos jobs vive no D1 (worker); o job
// reporta progresso/conclusão directamente ao worker.
//
// Variáveis de ambiente (todas já existentes):
//   ADMIN_API_KEY                                  — chave esperada em x-api-key (obrigatória)
//   GH_WORKFLOW_FILE / GH_WORKFLOW_REF             — workflow e branch a disparar
//   GH_ACCOUNT_1_OWNER / _REPO                     — repositório com o workflow
//   GH_ACCOUNT_1_APP_ID / _INSTALLATION_ID / _PRIVATE_KEY   — modo GitHub App (preferido)
//   GH_ACCOUNT_1_TOKEN                             — modo PAT (legado)
//   (contas 2..N continuam suportadas pelo loader se existirem; não são necessárias)
//   PORT / RENDER_EXTERNAL_URL                     — fornecidas pelo Render

import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));

const PORT = parseInt(process.env.PORT || process.env.DISPATCHER_PORT || '3002', 10);
const WORKFLOW_FILE = process.env.GH_WORKFLOW_FILE || 'process.yml';
const WORKFLOW_REF = process.env.GH_WORKFLOW_REF || 'main';
const ADMIN_KEY = process.env.ADMIN_API_KEY || '';
const MAX_INPUT_CHARS = 60000;           // limite do GitHub: 65535 por input

// ── Autenticação do GitHub: App (JWT -> installation token) ou PAT ───────────
function buildAppJwt(appId, privateKeyPem) {
  const now = Math.floor(Date.now() / 1000);
  const b64url = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const unsigned = `${b64url({ alg: 'RS256', typ: 'JWT' })}.${b64url({ iat: now - 60, exp: now + 9 * 60, iss: appId })}`;
  return `${unsigned}.${crypto.sign('RSA-SHA256', Buffer.from(unsigned), privateKeyPem).toString('base64url')}`;
}

const tokenCache = new Map();
async function installationToken(account) {
  const hit = tokenCache.get(account.id);
  if (hit && hit.expiresAt - Date.now() > 5 * 60 * 1000) return hit.token;
  const r = await fetch(`https://api.github.com/app/installations/${account.installationId}/access_tokens`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${buildAppJwt(account.appId, account.privateKey)}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
  });
  if (!r.ok) throw new Error(`installation token: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
  const data = await r.json();
  tokenCache.set(account.id, { token: data.token, expiresAt: new Date(data.expires_at).getTime() });
  return data.token;
}
const resolveToken = (account) => (account.mode === 'app' ? installationToken(account) : account.token);

function loadAccounts() {
  const accounts = [];
  for (let n = 1; ; n++) {
    const owner = process.env[`GH_ACCOUNT_${n}_OWNER`];
    const repo = process.env[`GH_ACCOUNT_${n}_REPO`];
    if (!owner || !repo) break;
    const appId = process.env[`GH_ACCOUNT_${n}_APP_ID`];
    const installationId = process.env[`GH_ACCOUNT_${n}_INSTALLATION_ID`];
    const pk = process.env[`GH_ACCOUNT_${n}_PRIVATE_KEY`];
    const token = process.env[`GH_ACCOUNT_${n}_TOKEN`];
    if (appId && installationId && pk) {
      accounts.push({ id: n, owner, repo, mode: 'app', appId, installationId, privateKey: pk.replace(/\\n/g, '\n'), lastUsed: 0 });
    } else if (token) {
      accounts.push({ id: n, owner, repo, mode: 'pat', token, lastUsed: 0 });
    } else {
      console.error(`Conta ${n} (${owner}) sem credenciais válidas — ignorada.`);
      break;
    }
  }
  return accounts;
}

const accounts = loadAccounts();
if (accounts.length === 0) {
  console.error('ERRO: nenhuma conta GitHub configurada (GH_ACCOUNT_1_*).');
  process.exit(1);
}
const pickAccount = () => [...accounts].sort((a, b) => a.lastUsed - b.lastUsed)[0];

// ── Dispatch com jitter entre chamadas e retry só em 429/5xx ─────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class DispatchQueue {
  constructor(minMs, maxMs) { this.minMs = minMs; this.maxMs = maxMs; this.tail = Promise.resolve(); this.last = 0; }
  enqueue(task) {
    const run = this.tail.then(async () => {
      const jitter = this.minMs + Math.random() * (this.maxMs - this.minMs);
      const wait = jitter - (Date.now() - this.last);
      if (this.last > 0 && wait > 0) await sleep(wait);
      this.last = Date.now();
      return task();
    });
    this.tail = run.then(() => {}, () => {});
    return run;
  }
}
const queue = new DispatchQueue(3000, 8000);

async function triggerWorkflow(account, inputs) {
  const token = await resolveToken(account);
  return fetch(`https://api.github.com/repos/${account.owner}/${account.repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'catalog-dispatcher',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ref: WORKFLOW_REF, inputs }),
  });
}

async function triggerWithRetry(account, inputs, attempts = 3) {
  let res;
  for (let i = 1; i <= attempts; i++) {
    res = await queue.enqueue(() => triggerWorkflow(account, inputs));
    if (res.ok || !(res.status === 429 || res.status >= 500)) return res;
    if (i < attempts) await sleep(Math.pow(2, i) * 1000 + Math.random() * 1000);
  }
  return res;
}

// ── Autenticação (falha fechada: sem ADMIN_API_KEY nada é aceite) ────────────
function safeEqual(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}
function auth(req, res, next) {
  if (!ADMIN_KEY) return res.status(503).json({ error: 'ADMIN_API_KEY não configurada' });
  if (!safeEqual(req.headers['x-api-key'] || '', ADMIN_KEY)) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── POST /dispatch  { job_id, params:{...} } ─────────────────────────────────
// Workflow tem 2 inputs (job_id, params) — muito abaixo do limite de 10.
app.post('/dispatch', auth, async (req, res) => {
  const { job_id: jobId, params } = req.body || {};
  if (typeof jobId !== 'string' || !/^job_[a-z0-9_]{3,80}$/.test(jobId)) return res.status(400).json({ error: 'job_id inválido' });
  if (!params || typeof params !== 'object' || Array.isArray(params)) return res.status(400).json({ error: 'params obrigatório (objecto)' });

  const paramsStr = JSON.stringify(params);
  if (paramsStr.length > MAX_INPUT_CHARS) return res.status(413).json({ error: 'params demasiado grande' });

  const account = pickAccount();
  try {
    const r = await triggerWithRetry(account, { job_id: jobId, params: paramsStr });
    if (!r.ok) {
      const t = await r.text();
      console.error(`[dispatch] GitHub ${r.status}: ${t.slice(0, 300)}`);
      return res.status(502).json({ error: `GitHub dispatch falhou (${r.status})`, details: t.slice(0, 300) });
    }
    account.lastUsed = Date.now();
    console.log(`[dispatch] ok job=${jobId} → ${account.owner}/${account.repo} (${WORKFLOW_FILE}@${WORKFLOW_REF})`);
    res.json({ ok: true, job_id: jobId, workflow: WORKFLOW_FILE });
  } catch (e) {
    console.error(`[dispatch] erro: ${e.message}`);
    res.status(500).json({ error: 'erro ao despachar' });
  }
});

app.get('/health', (_req, res) => res.json({ ok: true, workflow: `${WORKFLOW_FILE}@${WORKFLOW_REF}`, accounts: accounts.length, uptime: process.uptime() }));

app.use((_req, res) => res.status(404).json({ error: 'Not Found' }));

// ── Start (+ keep-alive: o plano free do Render adormece após 15 min) ────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dispatcher na porta ${PORT} — ${accounts.length} conta(s) — ${WORKFLOW_FILE}@${WORKFLOW_REF}`);
  if (process.env.RENDER) {
    const selfUrl = process.env.RENDER_EXTERNAL_URL ? `${process.env.RENDER_EXTERNAL_URL}/health` : `http://localhost:${PORT}/health`;
    setInterval(() => fetch(selfUrl, { signal: AbortSignal.timeout(10000) }).catch((e) => console.warn(`[keep-alive] ${e.message}`)), 13 * 60 * 1000);
  }
});
