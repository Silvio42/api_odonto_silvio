const db = require('../database/dbConnection');

class MensalidadeModel {

static async getBeneficiarioMensalidade(idBeneficiario) {
    try {
      
      const data = await db.raw(
        `SELECT 
           TO_CHAR(MIN(hsspaga.dvencpaga), 'MMYYYY') AS mensalidade
         FROM 
           hssusupg, hsspaga
         WHERE 
           hssusupg.nnumeusua = ?
           AND hssusupg.nnumepaga = hsspaga.nnumepaga`,
        [idBeneficiario]
      );

      if (data.length > 0) {
        return { error: false, data: data };
      } else {
        return { error: false, data: null };
      }
    } catch (error) {
      console.error(error);
      return { error: true, message: error.message };
    }
  }
}
  module.exports = MensalidadeModel;
