const knex = require('knex');
const config = require('../../knexfile');

const dbConnection = knex(config.simulation);


module.exports = dbConnection;