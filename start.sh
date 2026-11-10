#!/bin/sh
APP_PORT="${PORT:-10000}"

echo "[start] A iniciar servidor unificado (porta ${APP_PORT})..."
node dispatcher.js &
NODE_PID=$!

# Aguardar a porta estar realmente activa (até 30s) — elimina race condition
echo "[start] A aguardar porta ${APP_PORT}..."
i=0
while [ $i -lt 30 ]; do
  nc -z 127.0.0.1 "$APP_PORT" 2>/dev/null && break
  sleep 1
  i=$((i + 1))
done

if ! nc -z 127.0.0.1 "$APP_PORT" 2>/dev/null; then
  echo "[start] ERRO: servidor não abriu porta ${APP_PORT} em 30s — a abortar"
  kill "$NODE_PID" 2>/dev/null
  exit 1
fi
echo "[start] Porta ${APP_PORT} activa."


wait $NODE_PID
