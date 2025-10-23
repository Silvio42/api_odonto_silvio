'use strict';
try { require('dotenv').config(); } catch {}

const fs   = require('fs');
const path = require('path');
const db   = require(path.join(__dirname, '../../src/database/dbConnection'));

const args  = process.argv.slice(2);
const limit = (() => {
  const i = args.indexOf('--limit');
  const v = i >= 0 ? Number(args[i+1]) : null;
  return (v && v > 0) ? v : null;
})();

(async () => {
  try {
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
    const rs = await db.raw(sql, { limite: limit ?? 10 });
    const rows = Array.isArray(rs) ? rs : (rs?.rows || []);

    if (!rows?.length) {
      console.error('⚠️  Nenhuma empresa ativa encontrada.');
      process.exit(1);
    }

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

    if (!payloads.length) {
      console.error('⚠️  Nenhum payload válido (nome vazio/CNPJ inválido).');
      process.exit(1);
    }

    const outDir = path.join(process.cwd(), 'out');
    fs.mkdirSync(outDir, { recursive: true });
    const ts = new Date().toISOString().replace(/[-:T.Z]/g,'').slice(0,14);
    const file = path.join(outDir, `departamentos-${ts}.json`);
    fs.writeFileSync(file, JSON.stringify(payloads, null, 2), 'utf-8');

    console.log(`✅ Gerado: ${payloads.length} empresas`);
    console.log('💾 Arquivo:', path.relative(process.cwd(), file));
    process.exit(0);
  } catch (e) {
    console.error('❌ Erro ao gerar empresas:', e?.message || e);
    process.exit(1);
  } finally {
    try { await db.destroy?.(); } catch {}
  }
})();
