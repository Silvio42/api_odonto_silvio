// scripts/company-json-legacy.js
'use strict';

/**
 * Gera o JSON do /departamento obedecendo as regras do legado:
 * - Busca empresa via Oracle (HSSTITU/HSSEMPR) e, se informado, via HSSUSUA (nnumeusua)
 * - cd_empresa derivado de HSSTITU.CNATUTITU (como no legado)
 * - NUNCA envia para Odontogroup; só salva em out/departamento-<cnpj>.json
 *
 * Uso:
 *   node scripts/company-json-legacy.js                         # pega 1ª empresa ativa (mais recente)
 *   node scripts/company-json-legacy.js --cnpj 72081144000120   # por CNPJ (14 dígitos)
 *   node scripts/company-json-legacy.js --nnumeusua 527624      # por beneficiário (titular/dependente)
 */

try { require('dotenv').config(); } catch {}

const fs   = require('fs');
const path = require('path');

// usa a MESMA conexão da sua API
const db = require(path.join(__dirname, '../src/database/dbConnection'));

const args = process.argv.slice(2);
const get = (k) => {
  const i = args.indexOf(k);
  return i >= 0 ? args[i + 1] : null;
};
const nnumeusuaArg = get('--nnumeusua');
const cnpjArg      = (get('--cnpj') || '').replace(/\D/g, '');

// mapeamento legado para cd_empresa (ajuste se seu legado for diferente)
function mapCdEmpresa(cnatutitu) {
  // valores comuns: '3' = Empresarial; '4' (ou outros) = Adesão
  if (cnatutitu === '3') return 27552;   // Coletivo Empresarial
  if (cnatutitu === '4') return 27543;   // Coletivo Adesão
  // fallback (alguns legados mandavam sempre 27552)
  return Number(process.env.CD_EMPRESA || 27552);
}

async function fetchByCnpj(cnpj) {
  const sql = `
    SELECT *
      FROM (
        SELECT DISTINCT
               EMP.CRAZAEMPR  AS NOME,
               EMP.C_CGCEMPR  AS CNPJ,
               TITU.CNATUTITU AS NAT,
               TITU.DCONTTITU  AS DCONTR
          FROM HSSTITU TITU
          JOIN HSSEMPR EMP ON TITU.NNUMEEMPR = EMP.NNUMEEMPR
         WHERE TITU.CSITUTITU = 'A'
           AND EMP.C_CGCEMPR = ?
         ORDER BY TITU.DCONTTITU DESC
      )
     WHERE ROWNUM = 1`;
  const rows = await db.raw(sql, [cnpj]);
  return Array.isArray(rows) ? rows[0] : (rows?.rows?.[0] || rows?.[0] || null);
}

async function fetchByNnumeusua(nnumeusua) {
  const sql = `
    SELECT *
      FROM (
        SELECT DISTINCT
               EMP.CRAZAEMPR  AS NOME,
               EMP.C_CGCEMPR  AS CNPJ,
               TITU.CNATUTITU AS NAT,
               TITU.DCONTTITU  AS DCONTR
          FROM HSSUSUA USU
          JOIN HSSTITU  TITU ON USU.NNUMETITU  = TITU.NNUMETITU
          JOIN HSSEMPR  EMP  ON TITU.NNUMEEMPR = EMP.NNUMEEMPR
         WHERE USU.NNUMEUSUA = ?
         ORDER BY TITU.DCONTTITU DESC
      )
     WHERE ROWNUM = 1`;
  const rows = await db.raw(sql, [nnumeusua]);
  return Array.isArray(rows) ? rows[0] : (rows?.rows?.[0] || rows?.[0] || null);
}

async function fetchFirstActive() {
  const sql = `
    SELECT *
      FROM (
        SELECT DISTINCT
               EMP.CRAZAEMPR  AS NOME,
               EMP.C_CGCEMPR  AS CNPJ,
               TITU.CNATUTITU AS NAT,
               TITU.DCONTTITU  AS DCONTR
          FROM HSSTITU TITU
          JOIN HSSEMPR EMP ON TITU.NNUMEEMPR = EMP.NNUMEEMPR
         WHERE TITU.CSITUTITU = 'A'
         ORDER BY TITU.DCONTTITU DESC
      )
     WHERE ROWNUM = 1`;
  const rows = await db.raw(sql);
  return Array.isArray(rows) ? rows[0] : (rows?.rows?.[0] || rows?.[0] || null);
}

(async () => {
  try {
    let row = null;

    if (cnpjArg) {
      if (cnpjArg.length !== 14) {
        console.error('❌ CNPJ inválido (precisa ter 14 dígitos).');
        process.exit(1);
      }
      row = await fetchByCnpj(cnpjArg);
    } else if (nnumeusuaArg) {
      row = await fetchByNnumeusua(nnumeusuaArg);
    } else {
      row = await fetchFirstActive();
    }

    if (!row) {
      console.error('⚠️  Nenhuma empresa encontrada para gerar o JSON.');
      process.exit(1);
    }

    const nome = (row.NOME || row.nome || '').toString().trim();
    const cnpj = (row.CNPJ || row.cnpj || '').toString().replace(/\D/g, '');
    const nat  = (row.NAT  || row.nat  || '').toString().trim();

    if (!nome || cnpj.length !== 14) {
      console.error('⚠️  Registro inválido (nome vazio ou CNPJ ≠ 14):', { nome, cnpj });
      process.exit(1);
    }

    const cd_empresa = mapCdEmpresa(nat);

    const payload = {
      cd_empresa,     // mapeado pelo CNATUTITU (legado)
      nome,           // HSSEMPR.CRAZAEMPR
      nr_cgc: cnpj,   // HSSEMPR.C_CGCEMPR (só dígitos)
      cd_orgao: null, // conforme manual
      cd_grupo: null
    };

    const outDir = path.join(process.cwd(), 'out');
    fs.mkdirSync(outDir, { recursive: true });
    const file = path.join(outDir, `departamento-${cnpj}.json`);
    fs.writeFileSync(file, JSON.stringify(payload, null, 2), 'utf-8');

    console.log('✅ Payload gerado (legado):');
    console.log(JSON.stringify(payload, null, 2));
    console.log('💾 Salvo em:', path.relative(process.cwd(), file));
    process.exit(0);
  } catch (e) {
    console.error('❌ Erro ao montar JSON da empresa:', e?.message || e);
    process.exit(1);
  } finally {
    try { await db.destroy?.(); } catch {}
  }
})();
