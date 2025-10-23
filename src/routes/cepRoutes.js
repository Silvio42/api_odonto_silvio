const express = require('express');
const router = express.Router();
const CepController = require('../controllers/CepServiceController');

router.get('/:cep', CepController.consultaCep);

module.exports = router;
