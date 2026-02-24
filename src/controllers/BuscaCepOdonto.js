// src/controllers/BuscaCepOdonto.js
'use strict';

require('dotenv').config();
const axios = require('axios');

async function buscarCepOdonto(cep) {
  const base = process.env.ODONTO_S4E_BASE;
  const token = process.env.ODONTO_S4E_TOKEN || process.env.S4E_TOKEN;
  const cepLimpo = (cep || '').toString().replace(/\D/g, '');

  if (!base || !token) {
    console.warn('[CEP] ODONTO_S4E_BASE ou ODONTO_S4E_TOKEN/S4E_TOKEN não configurados no .env');
    return null;
  }
  if (!cepLimpo) {
    console.warn('[CEP] CEP vazio ou inválido');
    return null;
  }

  const url = `${base}/api/redeatendimento/Endereco`;

  try {
    console.log(`[CEP] Buscando CEP ${cepLimpo} em ${url}`);
    // IMPORTANTE: método POST, com token e cep como query params (igual Postman)
    const resp = await axios.post(
      url,
      null,
      {
        params: { token, cep: cepLimpo },
        timeout: 15000,
      }
    );

    const body = resp.data || {};
    if (body.codigo !== 1 || !body.dados) {
      console.warn('[CEP] resposta inesperada:', JSON.stringify(body));
      return null;
    }

    return body.dados; // { IdTipoLogradouro, TipoLogradouro, Logradouro, ... }
  } catch (err) {
    console.error('[CEP] erro ao buscar CEP', cepLimpo, '-', err.message);
    return null;
  }
}

module.exports = { buscarCepOdonto };
