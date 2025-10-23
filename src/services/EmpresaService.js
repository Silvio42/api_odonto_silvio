var axios = require("axios").default;

module.exports = {
    async getEmpresa(token, cnpj) {
        try {
            if (!cnpj) {
                throw new Error("O CNPJ é obrigatório");
            }

            const options = {
                method: 'GET',
                url: process.env.ODONTO_EMPRESA,
                headers: {
                    Authorization: `Bearer ${token}` 
                },
                params: {
                    cnpj: cnpj, 
                    empresa : '27552'
                }
            };

            const api = await axios.request(options)
                .then(function (response) {
                    return { error: false, data: response.data };
                })
                .catch(function (error) {
                    console.error(error);
                    return { error: true, data: error.message || error };
                });
            return api;

        } catch (err) {
            console.log(err);
            return { error: true, message: err.message };
        }
    },

    async setEmpresa(token, nomeEmpresa, cnpjEmpresa) {
        try {

            const options = {
                method: 'POST',
                url: process.env.ODONTO_EMPRESA,
                headers: {
                    Authorization: `Bearer ${token}`
                },
                data: {
                    cd_empresa: 27552,
                    nome: nomeEmpresa,
                    nr_cgc: cnpjEmpresa,
                    cd_orgao: null,
                    cd_grupo: null
                }
            };

            const api = await axios.request(options)
                .then(function (response) {
                    return { error: false, data: response.data.token };
                })
                .catch(function (error) {
                    console.error(error);
                    return { error: true, data: error };
                })
            return api;
        } catch (err) {
            console.log(err);
        }
    }
};

