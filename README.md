# Dispatcher (Render)

Recebe `POST /dispatch` do worker Cloudflare (header `x-api-key`) e dispara o workflow de descoberta
no GitHub Actions. Stateless; uma conta GitHub. Ver `dispatcher.js` para as variáveis (todas já existentes).
