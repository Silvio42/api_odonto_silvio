// scripts/login-hom.js
'use strict';

// .env opcional
try { require('dotenv').config(); } catch {}

const fs = require('fs');
const path = require('path');
const https = require('https');
const axios = require('axios');

function pad(n){ return String(n).padStart(2, '0'); }
function formatPtBR(date) {
  const d = date;
  const dd = pad(d.getDate());
  const mm = pad(d.getMonth() + 1);
  const yyyy = d.getFullYear();
  const HH = pad(d.getHours());
  const MM = pad(d.getMinutes());
  const SS = pad(d.getSeconds());
  return `${dd}/${mm}/${yyyy}, ${HH}:${MM}:${SS}`;
}

function b64urlDecode(str) {
  // converte base64url -> base64 e decodifica
  str = str.replace(/-/g, '+').replace(/_/g, '/');
  const padLen = (4 - (str.length % 4)) % 4;
  str += '='.repeat(padLen);
  return Buffer.from(str, 'base64').toString('utf8');
}

(function main() {
  const url = process.env.ODONTO_URL_LOGIN || 'https://apiv3hom.odontogroup.com.br/api/login';
  const user = process.env.ODONTO_USER;
  const pass = process.env.ODONTO_PASS;
  const insecure = process.env.INSECURE === '1';
  const timeoutMs = Number(process.env.TIMEOUT_MS || 20000);

  if (!user || !pass) {
    console.error('Erro: defina ODONTO_USER e ODONTO_PASS no .env');
    process.exit(1);
  }

  (async () => {
    const agent = new https.Agent({ rejectUnauthorized: !insecure });
    const res = await axios.get(url, {
      params: { user, password: pass },
      timeout: timeoutMs,
      httpsAgent: agent,
      validateStatus: () => true
    });

    if (res.status < 200 || res.status >= 300) {
      console.error(`Erro login: HTTP ${res.status}`);
      process.exit(1);
    }

    // a API pode devolver { token: '...' } ou o token "cru"
    const body = res.data ?? {};
    const token = body.token || body.Token || (typeof body === 'string' ? body : '');

    if (!token) {
      console.error('Login OK, mas não encontrei o token no retorno.');
      process.exit(1);
    }

    // decodifica o JWT pra pegar o exp (epoch seconds)
    let expiresInText = '';
    try {
      const parts = token.split('.');
      if (parts.length >= 2) {
        const payloadJson = b64urlDecode(parts[1]);
        const payload = JSON.parse(payloadJson);
        if (payload && payload.exp) {
          const dt = new Date(payload.exp * 1000);
          expiresInText = formatPtBR(dt); // "dd/MM/yyyy, HH:mm:ss"
        }
      }
    } catch {
      // se der algo errado ao decodificar, só deixa vazio
      expiresInText = '';
    }

    const output = { token, expiresIn: expiresInText };

    // salva em arquivos
    const outDir = path.join(process.cwd(), 'out');
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(path.join(outDir, 'token_apiv3.txt'), token, 'utf-8');
    fs.writeFileSync(path.join(outDir, 'login_apiv3.json'), JSON.stringify(output, null, 2), 'utf-8');

    // imprime no formato da doc
    console.log(JSON.stringify(output, null, 2));
  })().catch(err => {
    console.error(`Erro login: ${err.message}`);
    process.exit(1);
  });
})();
