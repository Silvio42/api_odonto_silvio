// src/database/dbConnection.js
require('dotenv').config();
const knex = require('knex');
const knexfile = require('../../knexfile');

const db = knex(knexfile.production);

module.exports = db;
