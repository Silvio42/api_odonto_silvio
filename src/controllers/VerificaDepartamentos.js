// src/controllers/VerificaDepartamentos.js
require('dotenv').config();
const axios = require('axios');
const db = require('../database/dbConnection');
const fs = require('fs');
const path = require('path');

// -------- helpers de arquivo/data --------
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

function saveResult(resumo) {
  const { year, month, day, time } = getDateParts();
  const baseDir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'departamento',
    'check',
    year,
    month,
    day
  );
  ensureDir(baseDir);

  const fullPath = path.join(baseDir, `verificacao_${time}.json`);
  fs.writeFileSync(fullPath, JSON.stringify(resumo, null, 2), 'utf8');
  console.log('Relatório gerado em:', fullPath);
}

// -------- helpers gerais --------
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

// -------- banco local --------
async function buscarTodosDepartamentosLocais() {
  const result = await db.raw(`
    SELECT id_odonto, cnpj
      FROM odonto_depart
     WHERE cnpj IS NOT NULL
  `);
  return extractRows(result);
}

// -------- chamada na API (GET /departamento?empresa=27552&cnpj=XXXX) --------
async function getDepartamentoRemotoPorCnpj(cnpjLimpo) {
  const baseUrl = process.env.ODONTO_BASE_APIV3;
  const token   = process.env.ODONTO_APIV3_TOKEN;
  const empresa = process.env.ODONTO_EMPRESA_CODE || '27552';

  if (!baseUrl || !token) {
    throw new Error('ODONTO_BASE_APIV3 ou ODONTO_APIV3_TOKEN não configurados no .env');
  }

  const url = `${baseUrl}/departamento?empresa=${empresa}&cnpj=${cnpjLimpo}`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    timeout: 15000,
  });

  return resp.data; // ex: { depId, depNome, CNPJ, ... }
}

// -------- fluxo principal --------
async function verificarDepartamentos() {
  const locais = await buscarTodosDepartamentosLocais();
  console.log('Total de registros em odonto_depart:', locais.length);

  const ok = [];
  const inconsistentes = [];
  const errosApi = [];

  for (const row of locais) {
    const idLocal = row.ID_ODONTO ?? row.id_odonto ?? null;
    const cnpjLocal = onlyDigits(row.CNPJ ?? row.cnpj);

    if (!cnpjLocal) continue;

    try {
      const remoto = await getDepartamentoRemotoPorCnpj(cnpjLocal);

      const depIdRemoto =
        remoto.depId ?? remoto.depid ?? remoto.DepId ?? null;
      const cnpjRemoto = onlyDigits(
        remoto.CNPJ ?? remoto.cnpj ?? remoto.nr_cgc ?? remoto.NR_CGC
      );

      const baseItem = {
        idLocal,
        depIdRemoto,
        cnpjLocal,
        cnpjRemoto,
      };

      // nenhum dado útil na resposta
      if (!depIdRemoto && !cnpjRemoto) {
        inconsistentes.push({
          tipo: 'SEM_DADOS_API',
          ...baseItem,
          respostaBruta: remoto,
        });
        console.warn('SEM_DADOS_API', baseItem);
        continue;
      }

      // compara CNPJ e id_odonto
      const cnpjDif = cnpjLocal !== cnpjRemoto;
      const idDif =
        idLocal != null &&
        depIdRemoto != null &&
        String(idLocal) !== String(depIdRemoto);

      if (cnpjDif || idDif || idLocal == null) {
        inconsistentes.push({
          tipo: 'DIVERGENCIA',
          cnpjDiferente: cnpjDif,
          idDiferente: idDif,
          faltaIdLocal: idLocal == null,
          ...baseItem,
          respostaBruta: remoto,
        });
        console.warn('DIVERGENCIA', baseItem);
      } else {
        ok.push(baseItem);
      }
    } catch (err) {
      console.error(
        `Erro ao consultar CNPJ ${cnpjLocal}:`,
        err.response?.data || err.message
      );
      errosApi.push({
        cnpjLocal,
        idLocal,
        status: err.response?.status,
        resposta: err.response?.data || err.message,
      });
    }
  }

  const resumo = {
    totalLocais: locais.length,
    totalOk: ok.length,
    totalInconsistentes: inconsistentes.length,
    totalErrosApi: errosApi.length,
    inconsistentes,
    errosApi,
  };

  saveResult(resumo);
}

module.exports = { verificarDepartamentos };

if (require.main === module) {
  verificarDepartamentos()
    .then(() => console.log('Verificação finalizada.'))
    .catch((err) => console.error('Erro geral na verificação:', err))
    .finally(() => db.destroy());
}
