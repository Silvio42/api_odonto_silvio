require('dotenv').config({
    path: '.env'
  });
  
  module.exports = {
    simulation: {
      client: 'oracledb',
      connection: {
        host: '172.16.0.36',
        database: 'solussml',
        user:process.env.DB_USER,
        password:process.env.PSW
      }
    }
  };
  