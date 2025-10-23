// scripts/sml-sysdate.js
const oracledb = require('oracledb');
oracledb.thin = true;

(async () => {
  const cfg = { user: 'hss', password: 'hss', connectString: '172.16.0.36:1521/solussml' };
  try {
    const conn = await oracledb.getConnection(cfg);
    const r = await conn.execute(`select *
             from teste__ssml`);
    console.log('[SML] OK:', r.rows);
    await conn.close();
  } catch (e) {
    console.error('[SML] ERRO:', e.message || e);
    process.exit(1);
  }
})();
