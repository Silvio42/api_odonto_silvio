// knexfile.js
require('dotenv').config();

const { DB_USER, PSW, DB_CONNECT_STRING } = process.env;

if (!DB_USER || !PSW || !DB_CONNECT_STRING) {
  throw new Error('DB_USER, PSW ou DB_CONNECT_STRING não definidos no .env');
}

module.exports = {
  production: {
    client: 'oracledb',
    connection: {
      user: DB_USER,
      password: PSW,
      connectString: DB_CONNECT_STRING,
    },
    pool: { min: 0, max: 5 },
    migrations: {},
  },
};
