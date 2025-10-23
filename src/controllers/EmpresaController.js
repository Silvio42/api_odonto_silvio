const EmpresaService = require('../services/EmpresaService');

exports.consultaEmpresa = async (token, nome, cnpj) => {
  try {
      if (!cnpj) {
          throw new Error('CNPJ é obrigatório.');
      }
   

      const empresaInfo = await EmpresaService.getEmpresa(token, cnpj);

      if (empresaInfo.error) {
   
          console.log('Empresa não encontrada. Iniciando rotina de inserção...');


          const insereContrato = await EmpresaService.setEmpresa(token, nome, cnpj);

          return { error: false, data: insereContrato };
      }
      return { error: false, data: empresaInfo.data };
  } catch (error) {
      console.error('Erro interno ao consultar ou inserir a empresa:', error.message);
      return { error: true, data: error.message || 'Erro interno ao consultar ou inserir a empresa.' };
  }
};


exports.setEmpresa = async (token, nome, cnpj) => {
  try {
    if (!cnpj) {
      throw new Error('CNPJ é obrigatório.');
    }
console.log('token ',token);
    const response = await EmpresaService.setEmpresa(token, nome, cnpj);

    if (response.error) {
      throw new Error(`Erro ao inserir o CNPJ: ${response.message}`);
    }

    return response.data; 
  } catch (error) {
    console.error('Erro ao inserir o CNPJ:', error.message);
    throw new Error(error.message || 'Erro interno ao consultar o CNPJ.');
  }
}