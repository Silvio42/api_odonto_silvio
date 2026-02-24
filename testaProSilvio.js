// testaProSilvio.js
require('dotenv').config();
const db = require('./src/database/dbConnection');

(async () => {
  try {
    console.log('Testando SELECT na tabela TESTA_PRO_SILVIO (PRODUÇÃO)...');
    const result = await db.raw('SELECT * FROM testa_pro_silvio');
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error('Erro ao consultar testa_pro_silvio:', error);
  } finally {
    await db.destroy();
    console.log('Conexão encerrada.');
  }
})();
