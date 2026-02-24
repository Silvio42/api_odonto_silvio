// src/controllers/CorrigeDepartamentos.js
require('dotenv').config();
const axios = require('axios');
const db = require('../database/dbConnection');
const fs = require('fs');
const path = require('path');

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

function saveFixLog(payload) {
  const { year, month, day, time } = getDateParts();
  const baseDir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'departamento',
    'fix',
    year,
    month,
    day
  );
  ensureDir(baseDir);
  const file = path.join(baseDir, `corrigidos_${time}.json`);
  fs.writeFileSync(file, JSON.stringify(payload, null, 2), 'utf8');
  console.log('Log de correções em:', file);
}

async function buscarLocais() {
  const result = await db.raw(`
    SELECT id_odonto, cnpj
      FROM odonto_depart
     WHERE cnpj IS NOT NULL
  `);
  return extractRows(result);
}

async function getDepartamentoRemotoPorCnpj(cnpjLimpo) {
  const baseUrl = process.env.ODONTO_BASE_APIV3;
  const token   = process.env.ODONTO_APIV3_TOKEN;
  const empresa = process.env.ODONTO_EMPRESA_CODE || '27552';

  if (!baseUrl || !token) {
    throw new Error('ODONTO_BASE_APIV3 ou ODONTO_APIV3_TOKEN não configurados');
  }

  const url = `${baseUrl}/departamento?empresa=${empresa}&cnpj=${cnpjLimpo}`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    timeout: 15000,
  });

  return resp.data; // { depId, CNPJ, ... }
}

async function corrigirDepartamentos() {
  const locais = await buscarLocais();
  console.log('Total em odonto_depart:', locais.length);

  const atualizados = [];
  const ignorados   = [];
  const erros       = [];

  for (const row of locais) {
    const idLocal   = row.ID_ODONTO ?? row.id_odonto ?? null;
    const cnpjLocal = onlyDigits(row.CNPJ ?? row.cnpj);

    if (!cnpjLocal) continue;

    try {
      const remoto = await getDepartamentoRemotoPorCnpj(cnpjLocal);

      const depIdRemoto =
        remoto.depId ?? remoto.depid ?? remoto.DepId ?? null;
      const cnpjRemoto = onlyDigits(
        remoto.CNPJ ?? remoto.cnpj ?? remoto.nr_cgc ?? remoto.NR_CGC
      );

      if (!depIdRemoto || !cnpjRemoto || cnpjLocal !== cnpjRemoto) {
        ignorados.push({
          motivo: 'SEM_DADOS_COERENTES',
          idLocal,
          depIdRemoto,
          cnpjLocal,
          cnpjRemoto,
          respostaBruta: remoto,
        });
        console.warn('Ignorando (dados incoerentes):', { idLocal, cnpjLocal });
        continue;
      }

      // Se já está igual, não faz nada
      if (idLocal && String(idLocal) === String(depIdRemoto)) {
        continue;
      }

      // *** UPDATE VIA RAW (sem aspas, para não dar ORA-00942) ***
      await db.raw(
        `UPDATE odonto_depart
            SET id_odonto = :id_odonto
          WHERE cnpj = :cnpj`,
        { id_odonto: depIdRemoto, cnpj: cnpjLocal }
      );

      console.log(`Atualizado CNPJ ${cnpjLocal}: ${idLocal} -> ${depIdRemoto}`);
      atualizados.push({
        cnpj: cnpjLocal,
        idAntes: idLocal,
        idDepois: depIdRemoto,
      });
    } catch (err) {
      console.error(
        `Erro ao corrigir CNPJ ${cnpjLocal}:`,
        err.response?.data || err.message
      );
      erros.push({
        cnpj: cnpjLocal,
        idLocal,
        erro: err.response?.data || err.message,
      });
    }
  }

  const resumo = {
    totalLocais: locais.length,
    totalAtualizados: atualizados.length,
    totalIgnorados: ignorados.length,
    totalErros: erros.length,
    atualizados,
    ignorados,
    erros,
  };

  saveFixLog(resumo);
}

module.exports = { corrigirDepartamentos };

if (require.main === module) {
  corrigirDepartamentos()
    .then(() => console.log('Correção finalizada.'))
    .catch((err) => console.error('Erro geral na correção:', err))
    .finally(() => db.destroy());
}
