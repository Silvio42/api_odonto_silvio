const db = require('../database/dbConnection');

const LogService = {
  async saveLog(nomeBeneficiario, idBeneficiario, mensagem, jsonData) {
    try {
      await db('API_ODONTOGROUP_LOG').insert({
        NOME_BENEFICIARIO: nomeBeneficiario,
        ID_BENEFICIARIO: idBeneficiario,
        MENSAGEM: mensagem,
        JSON: JSON.stringify(jsonData),
      });
      console.log('Log salvo com sucesso em API_ODONTOGROUP_LOG.');
    } catch (error) {
      console.error('Erro ao salvar o log:', error.message);
    }
  },
};

module.exports = LogService;
