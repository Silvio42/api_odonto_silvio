const axios = require('axios');

class TokenService {
  constructor() {
    this.token = null;
    this.expiration = null;
  }

  async fetchToken() {
    try {
      const options = {
        method: 'GET',
        url: process.env.ODONTO_URL_LOGIN,
        params: {
          user: process.env.ODONTO_USER,
          password: process.env.ODONTO_PASS,
        },
      };

      const apiResponse = await axios.request(options);

      this.token = apiResponse.data.token;
      this.expiration = Date.now() + apiResponse.data.expires_in * 1000; // Define o tempo de expiração

      return this.token;
    } catch (error) {
      console.error('Erro ao obter token:', error.message);
      throw new Error('Erro ao obter token do cliente.');
    }
  }

  async getToken() {
    try {
      if (!this.token || Date.now() > this.expiration) {
        await this.fetchToken();
      }

      return this.token;
    } catch (error) {
      console.error('Erro ao acessar token:', error.message);
      throw new Error('Erro ao acessar token.');
    }
  }
}

module.exports = new TokenService();
