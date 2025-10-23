const axios = require("axios").default;

class CepService {
    /**
     * Obtém informações de um CEP.
     * @param {string} cep - O CEP que será consultado.
     * @returns {Promise<Object>} Retorna um objeto com os dados do CEP ou um erro.
     */
    static async getCep(cep) {
        try {
            if (!cep) {
                throw new Error("O CEP é obrigatório");
            }

            const options = {
                method: 'POST',
                url: process.env.ODONTO_CEP, 
                params: {
                    token: process.env.TOKEN_CEP, 
                    cep: cep
                }
            };

            const response = await axios.request(options);

            return { error: false, data: response.data };
        } catch (error) {
            console.error("Erro ao consultar CEP:", error.message || error);
            return { error: true, message: error.message || error };
        }
    }
}

module.exports = CepService;
