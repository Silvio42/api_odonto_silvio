const express = require('express');
const router = express.Router();
const cepRoutes = require('./cepRoutes');
const beneficiarioRoutes = require('./beneficiarioRoutes');
const mensalidadeRoutes = require('./mensalidadeRoutes');

router.get('/', (req, res) => {
  res.send('API funcionando!');
});

router.use('/cep', cepRoutes);
router.use('/beneficiario', beneficiarioRoutes);
router.use('/mensalidade', mensalidadeRoutes);
router.use('/incluir', beneficiarioRoutes);

module.exports = router;
