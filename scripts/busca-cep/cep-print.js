// scripts/cep-print.js
'use strict';
try { require('dotenv').config(); } catch {}

const https = require('https');
const axios = require('axios');

const TOKEN = process.env.S4E_TOKEN;
const BASE  = (process.env.S4E_BASE || 'https://odontogroup.s4e.com.br').replace(/\/$/, '');
const CEP   = (process.argv[2] || process.env.CEP || '01001000').replace(/\D/g, '');
const INSEC = process.env.INSECURE === '1';

if (!TOKEN) { console.error('S4E_TOKEN ausente no .env'); process.exit(1); }
if (CEP.length !== 8) { console.error('CEP inválido (8 dígitos)'); process.exit(1); }

const agent = new https.Agent({ rejectUnauthorized: !INSEC });
const paths = ['/api/redeatendimento/Endereco', '/api/redeAtendimento/Endereco'];

function pickDados(obj = {}) {
  return obj.dados || obj.Dados || obj.data || obj.Data || obj;
}

(async () => {
  for (const p of paths) {
    const url = `${BASE}${p}?token=${encodeURIComponent(TOKEN)}&cep=${CEP}`;
    try {
      const res = await axios.post(url, null, {
        headers: { 'Accept': 'application/json' },
        httpsAgent: agent,
        timeout: 20000,
        validateStatus: () => true
      });

      if (res.status === 200 && res.data) {
        const d = pickDados(res.data);
        const endereco = {
          cep: CEP,
          IdTipoLogradouro: d.IdTipoLogradouro,
          TipoLogradouro:   d.TipoLogradouro,
          Logradouro:       d.Logradouro,
          IdBairro:         d.IdBairro,
          Bairro:           d.Bairro,
          IdMunicipio:      d.IdMunicipio,
          Municipio:        d.Municipio,
          IdUf:             d.IdUf,
          Uf:               d.Uf,
          CodigoMunicipioIBGE: d.CodigoMunicipioIBGE,
        };
        console.log(JSON.stringify({ ok: true, url, endereco, raw: res.data }, null, 2));
        process.exit(0);
      } else {
        // útil pra ver quando toma 404/405
        console.error(`[${res.status}] -> ${url}`);
      }
    } catch (e) {
      console.error(`[falha] ${url} :: ${e?.message || e}`);
    }
  }
  console.error('CEP ERRO: nenhuma rota retornou 200');
  process.exit(1);
})();
