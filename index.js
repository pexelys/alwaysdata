// index.js — ponto de entrada único
// Importa os 3 apps, monta-os num único Express na porta do Render.
// Cada ficheiro exporta a sua app/server sem chamar .listen().
import express from 'express';
import { app as polygonApp }    from './server.js';
import { app as dispatcherApp } from './dispatcher.js';

// FIX: proxy.js desligado — Cloudflare redireciona /{jobId}/... directamente
// para a Release pública do GitHub (Single Redirects), sem passar pelo Render.

const PORT = process.env.PORT || '10000';

const main = express();

// CORS é tratado dentro de cada sub-app (dispatcherApp já tem o seu próprio
// middleware). Não duplicar aqui para evitar headers conflitantes.

// Dispatcher primeiro — tem CORS configurado e define /health, /status,
// /dispatch, /webhook, /jobs. Precisa de vir antes do polygonApp porque
// ambos definem GET /health; o Express para no primeiro que responder.
// Se polygonApp vier primeiro, o /health é servido sem headers CORS e o
// browser bloqueia (ERR_FAILED 200 OK com "No Access-Control-Allow-Origin").
main.use(dispatcherApp);

// Polygon a seguir — define /polygon/*, /tron/* (prefixos completos internos).
main.use(polygonApp);

// Catch-all — proxy desligado, devolve 404 (Cloudflare já não envia tráfego aqui)
main.use('/', (req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

main.listen(PORT, '0.0.0.0', () => {
  console.log(`[index] servidor unificado na porta ${PORT}`);
  console.log(`  /polygon/* /tron/*              → polygon-microservice`);
  console.log(`  /dispatch /webhook /jobs /status → dispatcher`);
  console.log(`  /*                               → 404 (proxy desligado)`);

  // Keep-alive unificado
  if (process.env.RENDER || process.env.KEEP_ALIVE === 'true') {
    const selfUrl = process.env.RENDER_EXTERNAL_URL
      ? `${process.env.RENDER_EXTERNAL_URL}/health`
      : `http://localhost:${PORT}/health`;

    setInterval(async () => {
      try {
        await fetch(selfUrl, { signal: AbortSignal.timeout(5000) });
        console.log(`[keep-alive] ping → ${new Date().toISOString()}`);
      } catch (e) {
        console.warn(`[keep-alive] falhou: ${e.message}`);
      }
    }, 13 * 60 * 1000);
    console.log(`  keep-alive → ${selfUrl}`);
  }

  // FIX: scanner automático de pagamentos pendentes.
  // Sem isto, um pagamento só confirma se o utilizador ficar com a aba do
  // QR code aberta (polling em GET /api/payments/status/:id) — se fechar
  // a aba depois de enviar o USDT, a assinatura nunca activava sozinha.
  // POST /api/payments/scan já existe no EdgeOne para isto; só faltava
  // algo a chamá-lo periodicamente. Reaproveita o mesmo processo Node
  // sempre-ligado do Render que já faz o keep-alive acima.
  const scanApiUrl = (process.env.PIXGO_API_URL || '').replace(/\/api\/?$/, '').replace(/\/$/, '');
  const scanApiKey = process.env.ADMIN_API_KEY_MASTER;

  if (scanApiUrl && scanApiKey) {
    const scanIntervalMs = parseInt(process.env.PAYMENT_SCAN_INTERVAL_MS || '', 10) || 5 * 60 * 1000;

    setInterval(async () => {
      try {
        const res = await fetch(`${scanApiUrl}/api/payments/scan`, {
          method:  'POST',
          headers: { 'x-api-key': scanApiKey },
          signal:  AbortSignal.timeout(20_000),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          console.warn(`[payment-scan] respondeu ${res.status}:`, data);
        } else if (data.confirmed > 0 || data.errors > 0) {
          console.log(`[payment-scan] scanned=${data.scanned} confirmed=${data.confirmed} expired=${data.expired} errors=${data.errors}`);
        }
      } catch (e) {
        console.warn(`[payment-scan] falhou: ${e.message}`);
      }
    }, scanIntervalMs);

    console.log(`  payment-scan → ${scanApiUrl}/api/payments/scan (a cada ${scanIntervalMs / 1000}s)`);
  } else {
    console.warn('[payment-scan] PIXGO_API_URL e/ou ADMIN_API_KEY_MASTER não definidos — scanner automático DESLIGADO (pagamentos só confirmam com a aba do QR aberta)');
  }
});
