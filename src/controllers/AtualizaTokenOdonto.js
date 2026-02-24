// src/controllers/AtualizaTokenOdonto.js
'use strict';

const path = require('path');
const fs = require('fs');
const axios = require('axios');

// Carrega SEMPRE o .env da raiz do projeto
require('dotenv').config({
  path: path.join(__dirname, '..', '..', '.env'),
});

async function AtualizaTokenOdonto() {
  const base = process.env.ODONTO_BASE_APIV3;
  const user = process.env.ODONTO_USER;
  const pass = process.env.ODONTO_PASS;

  if (!base || !user || !pass) {
    console.error(
      '[TOKEN] ODONTO_BASE_APIV3 / ODONTO_USER / ODONTO_PASS não configurados no .env'
    );
    return;
  }

  // Usa a senha EXATAMENTE como está no .env (sem encode, sem alterar nada)
  const url = `${base}/login?user=${user}&password=${pass}`;

  console.log('[TOKEN] Buscando novo token em:', url);
  console.log('[TOKEN] user =', user, 'pass.length =', pass.length);

  try {
    const resp = await axios.get(url, { timeout: 20000 });
    const body = resp.data || {};

    console.log('[TOKEN] Token obtido com sucesso.');
    console.log('[TOKEN] Campo expiresIn:', body.expiresIn);

    const token = body.token;
    const expiresIn = body.expiresIn;

    if (!token) {
      console.error('[TOKEN] Resposta sem campo token:', body);
      process.exitCode = 1;
      return;
    }

    const envPath = path.join(__dirname, '..', '..', '.env');
    let envContent = fs.readFileSync(envPath, 'utf8');

    // Atualiza ou inclui ODONTO_APIV3_TOKEN
    if (/^ODONTO_APIV3_TOKEN=.*/m.test(envContent)) {
      envContent = envContent.replace(
        /^ODONTO_APIV3_TOKEN=.*/m,
        `ODONTO_APIV3_TOKEN=${token}`
      );
    } else {
      envContent += `\nODONTO_APIV3_TOKEN=${token}`;
    }

    // Atualiza ou inclui ODONTO_APIV3_TOKEN_EXPIRES (mantém o formato que a API devolve)
    if (expiresIn) {
      if (/^ODONTO_APIV3_TOKEN_EXPIRES=.*/m.test(envContent)) {
        envContent = envContent.replace(
          /^ODONTO_APIV3_TOKEN_EXPIRES=.*/m,
          `ODONTO_APIV3_TOKEN_EXPIRES=${expiresIn}`
        );
      } else {
        envContent += `\nODONTO_APIV3_TOKEN_EXPIRES=${expiresIn}`;
      }
    }

    fs.writeFileSync(envPath, envContent, 'utf8');
    console.log('[TOKEN] .env atualizado com sucesso.');
  } catch (err) {
    if (err.response) {
      console.error('[TOKEN] Erro ao atualizar token:', err.message);
      console.error('[TOKEN] Status:', err.response.status);
      console.error('[TOKEN] Body  :', err.response.data);
    } else {
      console.error('[TOKEN] Erro ao atualizar token:', err.message);
    }
    process.exitCode = 1;
  }
}

if (require.main === module) {
  AtualizaTokenOdonto();
}

module.exports = { AtualizaTokenOdonto };
