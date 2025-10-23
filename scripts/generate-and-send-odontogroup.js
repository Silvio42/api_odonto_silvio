// scripts/generate-and-send-odontogroup.js
'use strict';
try { require('dotenv').config(); } catch {}

const fs    = require('fs');
const path  = require('path');
const axios = require('axios');

// === dbConnection (ajuste automático de caminho a partir de scripts/) ===
let db;
for (const p of ['../src/database/dbConnection', '../../src/database/dbConnection', './src/database/dbConnection']) {
  try { db = require(path.join(__dirname, p)); break; } catch {}
}
if (!db) throw new Error('dbConnection não encontrado (src/database/dbConnection.js)');

// === CLI args ===
const args  = process.argv.slice(2);
const getArg = (k, d=null) => { const i=args.indexOf(k); return i>=0 && args[i+1] && !args[i+1].startsWith('--') ? args[i+1] : d; };
const limit = (() => { const i=args.indexOf('--limit'); const v=i>=0?Number(args[i+1]):null; return (v&&v>0)?v:10; })();
const outDir = getArg('--out', path.join(process.cwd(), 'out'));
const delayMs = Number(getArg('--delay','100')) || 100;

// === ENV ===
const env = (k, d='') => (process.env[k] ?? d).toString().trim();
const ODONTO_URL_LOGIN = env('ODONTO_URL_LOGIN');   // ex: https://homapiv3.odontogroup.com.br/api/login
const ODONTO_EMPRESA   = env('ODONTO_EMPRESA');     // ex: https://homapiv3.odontogroup.com.br/api/departamento
const ODONTO_USER      = env('ODONTO_USER');
const ODONTO_PASS      = env('ODONTO_PASS');

const sleep = ms => new Promise(r=>setTimeout(r,ms));

(async () => {
  try {
    /* 1) SELECT ORACLE → linhas */
    let sql = `
      SELECT *
      FROM (
        SELECT '27552' AS Cd_empresa,
               MIN(CRAZAEMPR) AS nome,
               C_CGCEMPR AS Nr_cgc,
               NULL AS Cd_orgao,
               NULL AS Cd_grupo,
               2 AS tpempresa,
               2 AS classificacao
        FROM HSSEMPR, HSSTITU
        WHERE HSSEMPR.Nnumeempr = HSSTITU.Nnumeempr
          AND HSSEMPR.csituempr = 'A'
          AND HSSEMPR.C_CGCEMPR IS NOT NULL
          AND HSSTITU.cnatutitu IN (3)
        GROUP BY HSSEMPR.C_CGCEMPR
      )
      WHERE ROWNUM <= :limite
    `;
    const rs = await db.raw(sql, { limite: limit });
    const rows = Array.isArray(rs) ? rs : (rs?.rows || []);
    if (!rows.length) throw new Error('Nenhuma empresa ativa encontrada.');

    /* 2) Normalizar → payloads */
    const payloads = [];
    for (const r of rows) {
      const nome  = (r.nome ?? r.NOME ?? '').toString().trim();
      const cnpj  = ((r.nr_cgc ?? r.NR_CGC) ?? '').toString().replace(/\D/g, '');
      const cdEmp = (r.Cd_empresa ?? r.CD_EMPRESA ?? '').toString().trim();
      const tp    = (r.tpempresa ?? r.TPEMPRESA ?? null);
      const cls   = (r.classificacao ?? r.CLASSIFICACAO ?? null);
      if (!nome || cnpj.length !== 14) continue;

      payloads.push({
        cd_empresa: cdEmp ? Number(cdEmp) : null,
        nome,
        nr_cgc: cnpj,
        cd_orgao: null,
        cd_grupo: null,
        tpempresa: tp != null ? Number(tp) : null,
        classificacao: cls != null ? Number(cls) : null
      });
    }
    if (!payloads.length) throw new Error('Nenhum payload válido (nome vazio/CNPJ inválido).');

    /* 3) Salvar gerado (fixo) */
    fs.mkdirSync(outDir, { recursive: true });
    const fileGerado = path.join(outDir, 'departamentos.json');
    fs.writeFileSync(fileGerado, JSON.stringify(payloads, null, 2), 'utf-8');
    console.log(`✅ Gerado: ${payloads.length} empresas`);
    console.log('💾 Arquivo:', path.relative(process.cwd(), fileGerado));

    /* 4) Login + envio */
    const login = async () => {
      const url = `${ODONTO_URL_LOGIN}?user=${encodeURIComponent(ODONTO_USER)}&password=${encodeURIComponent(ODONTO_PASS)}`;
      const { data } = await axios.get(url, { timeout: 15000 /*, proxy:false*/ });
      if (!data?.token) throw new Error('Login sem token');
      return data.token;
    };
    const postDepartamento = async (token, body) => {
      return axios.post(ODONTO_EMPRESA, body, {
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        timeout: 20000,
        // proxy: false,
      });
    };

    let token = await login();
    const successes = [];
    const failures  = [];

    for (let i = 0; i < payloads.length; i++) {
      const body = payloads[i];
      try {
        let res = await postDepartamento(token, body);
        if (res.status === 401) {
          token = await login();
          res = await postDepartamento(token, body);
        }
        successes.push({ index: i+1, input: body, status: res.status, output: res.data });
        console.log(`   ✓ [${i+1}/${payloads.length}] depId=${res.data?.depId ?? 'n/a'}`);
      } catch (err) {
        const status = err?.response?.status ?? 0;
        const data   = err?.response?.data ?? err?.message ?? String(err);
        failures.push({ index: i+1, input: body, status, error: data });
        console.log(`   ✗ [${i+1}/${payloads.length}] status=${status}`);
      }
      if (delayMs > 0) await sleep(delayMs);
    }

    /* 5) Salvar retornos (fixos) */
    const fileOk   = path.join(outDir, 'departamentos-ok.json');
    const fileErr  = path.join(outDir, 'departamentos-erro.json');
    fs.writeFileSync(fileOk,  JSON.stringify(successes, null, 2), 'utf-8');
    fs.writeFileSync(fileErr, JSON.stringify(failures,  null, 2), 'utf-8');

    console.log('✅ Envio concluído');
    console.log('   • sucesso:', successes.length, '→', path.relative(process.cwd(), fileOk));
    console.log('   • erros  :', failures.length,  '→', path.relative(process.cwd(), fileErr));
    process.exit(0);

  } catch (e) {
    console.error('❌ Erro geral:', e?.message || e);
    process.exit(1);
  } finally {
    try { await db.destroy?.(); } catch {}
  }
})();
