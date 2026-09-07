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

  // REMOVIDO (confirmado pelo utilizador): scanner automático de pagamentos
  // USDT/Polygon pendentes. Fazia sentido só na forma antiga de pagamento
  // (QR code + confirmação on-chain) — migração completa para Hotmart torna
  // isto obsoleto; POST /api/payments/scan já nem existe mais no EdgeOne
  // (removido do routes/payments.js na limpeza de cripto). Deixar este
  // setInterval ligado só batia num endpoint morto a cada 5min sem propósito
  // nenhum. keep-alive acima continua igual — não mexido.
});
