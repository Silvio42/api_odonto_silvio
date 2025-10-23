const express = require('express');
const routes = require('./routes/index'); // Certifique-se de que este caminho está correto
require('dotenv').config(); // Carrega as variáveis de ambiente

const app = express();

// Middleware para parsear JSON
app.use(express.json());

// Middleware para log de requisições (opcional)
app.use((req, res, next) => {
  console.log(`${req.method} ${req.url}`);
  next();
});

// Middleware para rotas
app.use('/api', routes); // Todas as rotas estarão sob o caminho "/api"

// Middleware para tratar erros (opcional)
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ message: 'Erro interno do servidor' });
});

module.exports = app;
