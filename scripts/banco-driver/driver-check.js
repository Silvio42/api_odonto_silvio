// scripts/driver-check.js
const oracledb = require('oracledb');
// oracledb.initOracleClient({libDir:'C:\\oracle\\product\\11.2.0\\client_1'}); // se DESCOMENTAR => Thick
oracledb.thin = true; // garanta Thin
(async () => {
  const cfg = { user: 'hss', password: 'hss', connectString: '172.16.0.36:1521/solussml' };
  console.log({ oracledbVersion: oracledb.versionString, thin: oracledb.thin, target: cfg.connectString });
  try {
    const c = await oracledb.getConnection(cfg);
    const r = await c.execute("select to_char(sysdate,'YYYY-MM-DD HH24:MI:SS') from dual");
    console.log('OK:', r.rows[0][0]); await c.close();
  } catch (e) { console.error('ERRO:', e.message); }
})();
