const express = require('express');
const router = express.Router();
const MensalidadeController = require('../controllers/MensalidadeController');

router.get('/:id', MensalidadeController.getMensalidade);

module.exports = router;
