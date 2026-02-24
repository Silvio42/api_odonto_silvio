// src/controllers/GeraDepartamentos.js
require('dotenv').config();
const axios = require('axios');
const db = require('../database/dbConnection');
const fs = require('fs');
const path = require('path');

// =============== LOG SIMPLES NO CONSOLE ===============
function log(level, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [DEPART] [${level}] ${msg}`);
}

// =============== HELPERS GERAIS ===============
function onlyDigits(s) {
  return String(s || '').replace(/\D/g, '');
}

function extractRows(result) {
  if (!result) return [];
  if (Array.isArray(result) && !Array.isArray(result[0])) return result;
  if (Array.isArray(result.rows)) return result.rows;
  if (Array.isArray(result[0])) return result[0];
  return [];
}

function getDateParts() {
  const now = new Date();
  const year = String(now.getFullYear());
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const time = [
    String(now.getHours()).padStart(2, '0'),
    String(now.getMinutes()).padStart(2, '0'),
    String(now.getSeconds()).padStart(2, '0'),
  ].join('');
  return { year, month, day, time };
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

// =============== LOGS EM ARQUIVO (SUCESSO / ERRO) ===============

function logSucessoDepartamento({ cnpj, nome, depId, origem, statusCode, response }) {
  const { year, month, day, time } = getDateParts();
  const cleanCnpj = onlyDigits(cnpj || 'sem_cnpj') || 'sem_cnpj';

  const dir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'departamento',
    'sucesso',
    year,
    month,
    day
  );
  ensureDir(dir);

  const fileName = `${time}_${cleanCnpj}.json`;
  const fullPath = path.join(dir, fileName);

  const payload = {
    cnpj: cleanCnpj,
    nome: nome || null,
    depId: depId ?? null,
    origem: origem || 'POST',
    httpStatus: statusCode ?? null,
    status: 'success',
    apiResponse: response || null,
    createdAt: new Date().toISOString(),
  };

  fs.writeFileSync(fullPath, JSON.stringify(payload, null, 2), 'utf8');
}

function logErroDepartamento({ cnpj, nome, statusCode, mensagem, response }) {
  const { year, month, day, time } = getDateParts();
  const cleanCnpj = onlyDigits(cnpj || 'sem_cnpj') || 'sem_cnpj';

  const dir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'departamento',
    'erro',
    year,
    month,
    day
  );
  ensureDir(dir);

  const fileName = `${time}_${cleanCnpj}.json`;
  const fullPath = path.join(dir, fileName);

  const payload = {
    cnpj: cleanCnpj,
    nome: nome || null,
    status: 'error',
    httpStatus: statusCode ?? null,
    mensagem: mensagem || null,
    apiResponse: response || null,
    createdAt: new Date().toISOString(),
  };

  fs.writeFileSync(fullPath, JSON.stringify(payload, null, 2), 'utf8');
}

// =============== BANCO: PENDENTES E MERGE ===============

async function buscarDepartamentosPendentes() {
  const result = await db.raw(`
  SELECT t.*
FROM (
    SELECT
        '27552' AS Cd_empresa,
        MIN(e.CRAZAEMPR) AS nome,
        COALESCE(e.C_CGCEMPR, e.CCAEPEMPR) AS Nr_cgc,
        NULL AS Cd_orgao,
        NULL AS Cd_grupo,
        2 AS tpempresa,
        2 AS classificacao,
        MAX(NVL2(e.C_CGCEMPR, 0, 1)) AS isCAEPF
    FROM HSSEMPR e
    JOIN HSSTITU i
      ON e.Nnumeempr = i.Nnumeempr
    WHERE e.csituempr = 'A'
      AND (e.C_CGCEMPR IS NOT NULL OR e.CCAEPEMPR IS NOT NULL)
      AND i.cnatutitu IN (3)
    GROUP BY COALESCE(e.C_CGCEMPR, e.CCAEPEMPR)
) t
WHERE NOT EXISTS (
    SELECT 1
    FROM odonto_depart d
    WHERE d.cnpj = t.Nr_cgc
)
  `);

  return extractRows(result);
}

function mapRowToBody(row) {
  const r = row || {};
  const get = (...keys) =>
    keys.reduce((v, k) => (v !== undefined ? v : r[k]), undefined);

  let nome = get('NOME', 'nome');
  if (nome != null) nome = String(nome).substring(0, 70);

  return {
    cd_empresa: get('CD_EMPRESA', 'Cd_empresa', 'cd_empresa'),
    nome,
    nr_cgc: get('NR_CGC', 'Nr_cgc', 'nr_cgc'),
    cd_orgao: get('CD_ORGAO', 'Cd_orgao', 'cd_orgao'),
    cd_grupo: get('CD_GRUPO', 'Cd_grupo', 'cd_grupo'),
    tpempresa: get('TPEMPRESA', 'tpempresa', 'Tpempresa'),
    classificacao: get('CLASSIFICACAO', 'classificacao', 'Classificacao'),
    isCAEPF: get('ISCAEPF', 'isCAEPF', 'ISCAEPF'),
  };
}

async function upsertOdontoDepart(depId, cnpjLimpo) {
  if (depId != null) {
    await db.raw(
      `MERGE INTO odonto_depart d
         USING (SELECT :cnpj AS cnpj, :id_odonto AS id_odonto FROM dual) s
            ON (d.cnpj = s.cnpj)
       WHEN MATCHED THEN
         UPDATE SET d.id_odonto = s.id_odonto
       WHEN NOT MATCHED THEN
         INSERT (id_odonto, cnpj) VALUES (s.id_odonto, s.cnpj)`,
      { cnpj: cnpjLimpo, id_odonto: depId }
    );
  } else {
    await db.raw(
      `INSERT INTO odonto_depart (cnpj)
         SELECT :cnpj FROM dual
          WHERE NOT EXISTS (
            SELECT 1 FROM odonto_depart d WHERE d.cnpj = :cnpj
          )`,
      { cnpj: cnpjLimpo }
    );
  }
}

// =============== CHAMADAS NA API ===============

async function getDepartamentoPorCnpj(cnpjLimpo) {
  const baseUrl = process.env.ODONTO_BASE_APIV3;
  const token = process.env.ODONTO_APIV3_TOKEN;
  const empresa = process.env.ODONTO_EMPRESA_CODE || '27552';

  const url = `${baseUrl}/departamento?empresa=${empresa}&cnpj=${cnpjLimpo}`;

  try {
    const resp = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      timeout: 15000,
    });

    const data = resp.data || {};
    const depid = data.depid ?? data.depId ?? data.DepId ?? null;

    return { ok: true, found: !!depid, depid, status: resp.status, data };
  } catch (err) {
    return {
      ok: false,
      found: false,
      depid: null,
      status: err.response?.status,
      data: err.response?.data,
      errorMessage: err.message,
    };
  }
}

async function postDepartamento(body, cnpjLimpo) {
  const baseUrl = process.env.ODONTO_BASE_APIV3;
  const token = process.env.ODONTO_APIV3_TOKEN;

  try {
    const resp = await axios.post(`${baseUrl}/departamento`, body, {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      timeout: 15000,
    });

    const data = resp.data || {};
    const depid = data.depid ?? data.depId ?? data.DepId ?? null;

    return { ok: true, depid, status: resp.status, data };
  } catch (err) {
    return {
      ok: false,
      depid: null,
      status: err.response?.status,
      data: err.response?.data,
      errorMessage: err.message,
    };
  }
}

// =============== FLUXO PRINCIPAL ===============

async function enviarDepartamentos() {
  const baseUrl = process.env.ODONTO_BASE_APIV3;
  const token = process.env.ODONTO_APIV3_TOKEN;

  if (!baseUrl || !token) {
    log('ERROR', 'Falta ODONTO_BASE_APIV3 ou ODONTO_APIV3_TOKEN no .env');
    return;
  }

  const rows = await buscarDepartamentosPendentes();
  log('INFO', `Departamentos pendentes: ${rows.length}`);

  if (!rows.length) {
    log('INFO', 'Nenhum departamento pendente para envio.');
    return;
  }

  let okCount = 0;
  let errCount = 0;

  for (const row of rows) {
    const body = mapRowToBody(row);
    const cnpjLimpo = onlyDigits(body.nr_cgc);
    const nome = body.nome || null;

    if (!cnpjLimpo) {
      log('WARN', `Linha sem CNPJ, pulando. Nome=${nome || ''}`);
      continue;
    }

    log('INFO', `Processando CNPJ=${cnpjLimpo} Nome="${nome || ''}"`);

    // 1) POST
    const postRes = await postDepartamento(body, cnpjLimpo);

    if (postRes.ok && postRes.depid) {
      await upsertOdontoDepart(postRes.depid, cnpjLimpo);
      logSucessoDepartamento({
        cnpj: cnpjLimpo,
        nome,
        depId: postRes.depid,
        origem: 'POST',
        statusCode: postRes.status,
        response: postRes.data,
      });
      log('SUCCESS', `POST OK CNPJ=${cnpjLimpo} depId=${postRes.depid}`);
      okCount++;
      continue;
    }

    // 2) Tratamento "já existe com esse CNPJ"
    const data = postRes.data;
    const msg =
      (Array.isArray(data) && JSON.stringify(data)) ||
      data?.Erro ||
      data?.error ||
      JSON.stringify(data || '');

    const jaExiste =
      postRes.status === 400 &&
      msg &&
      msg.includes('Já existe um Departamento cadastrado com esse CNPJ');

    if (jaExiste) {
      log('INFO', `CNPJ=${cnpjLimpo} já existe (400). Tentando GET para pegar depId...`);
      const getRes = await getDepartamentoPorCnpj(cnpjLimpo);

      if (getRes.ok && getRes.found && getRes.depid) {
        await upsertOdontoDepart(getRes.depid, cnpjLimpo);
        logSucessoDepartamento({
          cnpj: cnpjLimpo,
          nome,
          depId: getRes.depid,
          origem: 'GET',
          statusCode: getRes.status,
          response: getRes.data,
        });
        log(
          'SUCCESS',
          `CNPJ=${cnpjLimpo} já existia, depId sincronizado via GET=${getRes.depid}`
        );
        okCount++;
      } else {
        logErroDepartamento({
          cnpj: cnpjLimpo,
          nome,
          statusCode: getRes.status,
          mensagem: getRes.errorMessage || 'GET não retornou depId',
          response: getRes.data,
        });
        log(
          'WARN',
          `CNPJ=${cnpjLimpo} já existia, mas GET não retornou depId (status=${getRes.status || '??'}).`
        );
        errCount++;
      }
    } else {
      logErroDepartamento({
        cnpj: cnpjLimpo,
        nome,
        statusCode: postRes.status,
        mensagem: msg || postRes.errorMessage || 'Falha no POST',
        response: data,
      });
      log(
        'ERROR',
        `Falha POST CNPJ=${cnpjLimpo} status=${postRes.status || '??'} msg=${msg}`
      );
      errCount++;
    }
  }

  log('INFO', `Resumo do dia: sucesso=${okCount}, erros=${errCount}`);
}

module.exports = { enviarDepartamentos };

if (require.main === module) {
  enviarDepartamentos()
    .then(() => {
      log('INFO', 'Processo de departamentos finalizado (script standalone).');
    })
    .catch((err) => {
      log('ERROR', `Erro geral: ${err.message || err}`);
    })
    .finally(() => db.destroy());
}
