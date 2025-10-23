const express = require('express');
const router = express.Router();
const beneficiarioOdontoController = require('../controllers/BeneficiarioController');

router.get('/titular/:id', beneficiarioOdontoController.getBeneficiarioTitular);

router.get('/beneficiario/:id', beneficiarioOdontoController.getBeneficiarioAditivo);

router.post('/incluir', beneficiarioOdontoController.incluirBeneficiario);

router.post('/incluirVariosBeneficiarios', beneficiarioOdontoController.incluirVariosBeneficiarios);


module.exports = router;
