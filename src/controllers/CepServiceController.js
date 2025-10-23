const CepService = require('../services/CepService');


exports.consultaCep = async (cep, nome, nnumeusua) => {
  try {
    if (!cep) {
      throw new Error('CEP é obrigatório.');
    }

    const response = await CepService.getCep(cep);
    if (response.error) {

      throw new Error(`Erro ao consultar o CEP: ${response.message}`);

    }

    return response.data; // Retorna diretamente os dados do CEP
  } catch (error) {
    console.error('Erro ao consultar o CEP:', error.message);
    throw new Error(error.message || 'Erro interno ao consultar o CEP.');
  }
};
