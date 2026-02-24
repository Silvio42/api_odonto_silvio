// src/controllers/VerificaCpfOdonto.js
require('dotenv').config();

const knex = require('knex');
const axios = require('axios');

const knexConfig =
  require('../../knexfile')[process.env.NODE_ENV || 'production'];
const db = knex(knexConfig);

function log(msg) {
  console.log('[ODONTO_CPF_CHECK]', msg);
}

function extractRows(result) {
  if (!result) return [];
  if (Array.isArray(result) && !Array.isArray(result[0])) return result;
  if (Array.isArray(result.rows)) return result.rows;
  if (Array.isArray(result[0])) return result[0];
  return [];
}

function get(row, ...names) {
  for (const n of names) {
    if (row[n] !== undefined) return row[n];
    const up = n.toUpperCase();
    const low = n.toLowerCase();
    if (row[up] !== undefined) return row[up];
    if (row[low] !== undefined) return row[low];
  }
  return null;
}

function onlyDigits(v) {
  return (v || '').toString().replace(/\D/g, '');
}

// ============= SQL PARA BUSCAR OS BENEFICIÁRIOS =============

const SQL_BENEFICIARIOS = `
WITH registros_unicos AS (
  SELECT
         hssusua.cnomeusua                               AS nome,
         TO_CHAR(hssusua.dnascusua,'YYYY-MM-DD')         AS nascimento,
         hssusua.c_cpfusua                               AS cpf,
         hssusua.nnumeusua                               AS nnumeusua,

         odonto_depart.id_odonto                         AS departamento,
         hsstitu.nnumetitu,
         hssusua.ntituusua                               AS titular,

         ROW_NUMBER() OVER (
           PARTITION BY hssusua.nnumeusua
           ORDER BY hssusua.dinclusua DESC
         ) AS rn
    FROM hssusua,
         hsstitu,
         hssempr,
         hsspess,
         hssendp,
         hsstxusu,
         odonto_depart,
         (
           SELECT nnumepess,
                  contato,
                  tipo_contato
             FROM (
                   SELECT c.nnumepess,
                          c.contato,
                          c.tipo_contato,
                          ROW_NUMBER() OVER (
                            PARTITION BY c.nnumepess
                            ORDER BY c.ordem
                          ) rn
                     FROM (
                           SELECT hssfonp.nnumepess,
                                  hssfonp.cddd_fonp || hssfonp.cfonefonp AS contato,
                                  CASE 
                                    WHEN hssfonp.ctipofonp = 'E' THEN '8'
                                    WHEN hssfonp.ctipofonp = 'R' THEN '1'
                                    WHEN hssfonp.ctipofonp = 'C' THEN '1'
                                    WHEN hssfonp.ctipofonp = 'W' THEN '10'
                                  END AS tipo_contato,
                                  1 AS ordem
                             FROM hssfonp
                            WHERE hssfonp.cprinfonp = 'S'

                           UNION ALL

                           SELECT hssemap.nnumepess,
                                  hssemap.cmailemap AS contato,
                                  '50' AS tipo_contato,
                                  2    AS ordem
                             FROM hssemap
                            WHERE hssemap.cprinemap = 'S'
                         ) c
                 )
            WHERE rn = 1
         ) cont
   WHERE 0 = 0
     AND hsstxusu.dcanctxusu IS NULL
     AND hsstxusu.nnumetxmen IN (
           155616900,
           163198861,
           168980581,
           141287944,
           208002771
         )
     AND hssusua.csituusua = 'A'
     AND hssusua.nnumetitu = hsstitu.nnumetitu (+)
     AND hsstitu.nnumeempr = hssempr.nnumeempr (+)
     AND hssusua.nnumepess = hsspess.nnumepess (+)
     AND hsspess.nnumepess = hssendp.nnumepess(+)
     AND hssusua.nnumeusua = hsstxusu.nnumeusua (+)
     AND hssusua.nnumepess = cont.nnumepess(+)
     AND (hssempr.c_cgcempr = odonto_depart.cnpj
      OR hssempr.ccaepempr = odonto_depart.cnpj)
     AND hssusua.nnumeusua not IN (select NNUMEUSUA
  from odonto_associado_emp_chk
 where status_api = 'ENCONTRADO')
     
)
SELECT *
  FROM registros_unicos
 WHERE rn = 1
`;

// ============= FUNÇÃO PRA GRAVAR RESULTADO NO BANCO =============

async function gravarResultadoCheck({
  nnumeusua,
  cpf,
  nome,
  statusApi,
  httpStatus,
  msgRetorno,
  jsonRetorno,
}) {
  try {
    const msg = (msgRetorno || '').toString().substring(0, 4000);
    const jsonStr = jsonRetorno ? JSON.stringify(jsonRetorno) : null;

    await db.raw(
      `
      INSERT INTO odonto_associado_emp_chk
        (id_log,
         nnumeusua,
         cpf,
         nome,
         status_api,
         http_status,
         msg_retorno,
         json_retorno,
         dt_execucao)
      VALUES
        (seq_odonto_ass_emp_chk.NEXTVAL,
         ?, ?, ?, ?, ?, ?, ?, SYSDATE)
    `,
      [
        nnumeusua,
        cpf,
        nome,
        statusApi,
        httpStatus ? Number(httpStatus) : null,
        msg,
        jsonStr,
      ]
    );
  } catch (err) {
    log(
      `[ODONTO_CPF_CHECK] ERRO ao gravar log (nnumeusua=${nnumeusua}, cpf=${cpf}): ${err.message}`
    );
  }
}

// ============= CHAMADA DA API /api/associado-Emp =============

async function consultarAssociadoEmp(cpf, tokenApiv3) {
  const baseApiV3 = process.env.ODONTO_BASE_APIV3; // ex: https://apiv3.odontogroup.com.br/api
  if (!baseApiV3) throw new Error('ODONTO_BASE_APIV3 não configurado no .env');

  const empresasParam = '[27543, 27552]';
  const url = `${baseApiV3}/associado-Emp?cpf=${cpf}&empresas=${encodeURIComponent(
    empresasParam
  )}`;

  log(`[ODONTO_CPF_CHECK] GET associado-Emp: url=${url}`);

  try {
    const resp = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${tokenApiv3}`,
      },
      timeout: 30000,
    });

    const data = resp.data;
    const array = Array.isArray(data) ? data : [];

    const found = array.length > 0;

    return {
      found,
      httpStatus: resp.status,
      body: data,
      msg:
        found
          ? `Encontrado(s) ${array.length} registro(s) na API`
          : 'Nenhum registro encontrado na API',
    };
  } catch (error) {
    const status = error.response?.status || 'no-response';
    const body = error.response?.data || {};
    const msg = error.message || 'Erro ao chamar associado-Emp';

    return {
      found: false,
      httpStatus: status,
      body,
      msg,
      isError: true,
    };
  }
}

// ============= FUNÇÃO PRINCIPAL =============

async function verificarCpfsNaApi() {
  let tokenApiv3 = null;

  try {
    log('Buscando beneficiários no banco...');
    const result = await db.raw(SQL_BENEFICIARIOS);
    const rows = extractRows(result);
    log(`Total de linhas retornadas: ${rows.length}`);

    if (!rows.length) {
      log('Nenhum registro encontrado. Encerrando.');
      return;
    }

    tokenApiv3 = process.env.ODONTO_APIV3_TOKEN;
    if (!tokenApiv3) {
      throw new Error('ODONTO_APIV3_TOKEN não configurado no .env');
    }

    // cache por CPF pra não bater na API 2x pro mesmo documento
    const cacheCpf = new Map();

    for (const row of rows) {
      const nnumeusua = get(row, 'nnumeusua');
      const nome = get(row, 'nome');
      const cpf = onlyDigits(get(row, 'cpf'));

      if (!cpf) {
        log(`[ODONTO_CPF_CHECK] nnumeusua=${nnumeusua} sem CPF. Pulando.`);
        continue;
      }

      // se já consultamos esse CPF, reaproveita o resultado
      if (cacheCpf.has(cpf)) {
        const cached = cacheCpf.get(cpf);
        log(
          `[ODONTO_CPF_CHECK] CPF ${cpf} já consultado anteriormente. Reutilizando resultado (${cached.statusApi}).`
        );

        await gravarResultadoCheck({
          nnumeusua,
          cpf,
          nome,
          statusApi: cached.statusApi,
          httpStatus: cached.httpStatus,
          msgRetorno: cached.msg,
          jsonRetorno: cached.body,
        });

        continue;
      }

      log(
        `[ODONTO_CPF_CHECK] Consultando CPF=${cpf} (nnumeusua=${nnumeusua}, nome=${nome}) na API...`
      );

      const resultApi = await consultarAssociadoEmp(cpf, tokenApiv3);

      const statusApi = resultApi.isError
        ? 'ERRO'
        : resultApi.found
        ? 'ENCONTRADO'
        : 'NAO_ENCONTRADO';

      // salva no cache
      cacheCpf.set(cpf, {
        statusApi,
        httpStatus: resultApi.httpStatus,
        msg: resultApi.msg,
        body: resultApi.body,
      });

      // grava na tabela de auditoria
      await gravarResultadoCheck({
        nnumeusua,
        cpf,
        nome,
        statusApi,
        httpStatus: resultApi.httpStatus,
        msgRetorno: resultApi.msg,
        jsonRetorno: resultApi.body,
      });
    }

    log('Processo de verificação de CPFs concluído.');
  } catch (err) {
    console.error('[ODONTO_CPF_CHECK] Erro geral:', err.message || err);
  } finally {
    if (db && typeof db.destroy === 'function') {
      await db.destroy();
    }
  }
}

module.exports = { verificarCpfsNaApi };

if (require.main === module) {
  verificarCpfsNaApi();
}
