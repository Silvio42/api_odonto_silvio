const BeneficiarioOdonto = require('../models/BeneficiarioModel');
const MensalidadeBeneficiario = require('../models/MensalidadeModel');
const ConsultaCep = require('../controllers/CepServiceController');
const EmpresaController = require('../controllers/EmpresaController');
const TokenService = require('../services/TokenService');
const LogService = require('../services/LogService');
const axios = require('axios');

exports.getBeneficiarioTitular = async (req, res) => {
  try {
    const { id } = req.params;
    const response = await BeneficiarioOdonto.getBeneficiarioTitular(id);

    if (response.error) {
      return res.status(500).json({ message: response.message });
    }

    if (!response.data) {
      return res.status(404).json({ message: 'Beneficiário não encontrado' });
    }

    res.json(response.data);
  } catch (error) {
    console.error("Erro ao buscar titular:", error.message);
    res.status(500).json({ message: error.message });
  }
};

exports.getBeneficiarioAditivo = async (req, res) => {
  try {
    const { id } = req.params;
    const response = await BeneficiarioOdonto.getBeneficiarioAditivo(id);

    if (response.error) {
      return res.status(500).json({ message: response.message });
    }

    if (!response.data) {
      return res.status(404).json({ message: 'Beneficiário não encontrado' });
    }

    res.json(response.data);
  } catch (error) {
    console.error("Erro ao buscar aditivo:", error.message);
    res.status(500).json({ message: error.message });
  }
};

exports.incluirBeneficiario = async (idBeneficiario) => {
  try {
    const tokenResponse = await TokenService.getToken();
    const token = tokenResponse;

    const beneficiarioResponse = await BeneficiarioOdonto.getBeneficiario(idBeneficiario);
    if (!beneficiarioResponse || !beneficiarioResponse.data) {
      throw new Error('Beneficiário não encontrado.');
    }
    const beneficiario = beneficiarioResponse.data;

    let responsavelFinanceiro = beneficiario;
    if (beneficiario.TIPO_USUARIO !== 1) {
      const titularResponse = await BeneficiarioOdonto.getBeneficiarioTitular(beneficiario.TITULAR);
      if (titularResponse && titularResponse.data) {
        responsavelFinanceiro = titularResponse.data;
      }
    }

    const mensalidadeResponse = await MensalidadeBeneficiario.getBeneficiarioMensalidade(responsavelFinanceiro.TITULAR);
    responsavelFinanceiro.MENSALIDADE = mensalidadeResponse.data[0]?.MENSALIDADE;

    const cepInfo = await ConsultaCep.consultaCep(responsavelFinanceiro.CEP, beneficiario.NOME, beneficiario.NNUMEUSUA);
    if (!cepInfo || !cepInfo.dados) {
      throw new Error('CEP inválido ou não encontrado.');
    }
    const enderecoCep = cepInfo.dados;

    const empresaInfo = await EmpresaController.consultaEmpresa(token, responsavelFinanceiro.NOME_EMPRESA, responsavelFinanceiro.CNPJ_EMPRESA);
    if (!empresaInfo || empresaInfo.error) {
      throw new Error('Erro ao consultar informações da empresa.');
    }

    if (empresaInfo.error) {
      const empresaInserida = await EmpresaController.setEmpresa(token, responsavelFinanceiro.NOME_EMPRESA, responsavelFinanceiro.CNPJ_EMPRESA);
      if (empresaInserida.error) {
        throw new Error('Erro ao inserir empresa no sistema.');
      }
    }
    const dadosContato = await BeneficiarioOdonto.getBeneficiarioContato(responsavelFinanceiro.TITULAR);
    const responsavelFinanceiroContato = dadosContato.data;

    const idDepartamento = Array.isArray(empresaInfo.depID) && empresaInfo.depID.length > 0 && empresaInfo.depID[0] > 0
      ? empresaInfo.depID[0]
      : null;

      const contatoResponsavelFinanceiro = Array.isArray(responsavelFinanceiroContato)
      ? responsavelFinanceiroContato.map((record) => ({
          tipo: record.tipo_contato,
          dado: record.contato,
        }))
      : [];
    

    const dependentes = [
      {
        tipo: responsavelFinanceiro.TIPO_USUARIO,
        nome: responsavelFinanceiro.NOME,
        dataNascimento: responsavelFinanceiro.NASCIMENTO,
        cpf: responsavelFinanceiro.CPF,
        sexo: responsavelFinanceiro.SEXO,
        plano: 127,
        planoValor: "659",
        nomeMae: responsavelFinanceiro.NOME_MAE,
        MMYYYY1Pagamento: responsavelFinanceiro.MENSALIDADE,
      },
    ];

    if (beneficiario.TIPO_USUARIO !== 1) {
      dependentes.push({
        tipo: beneficiario.TIPO_USUARIO,
        nome: beneficiario.NOME,
        dataNascimento: beneficiario.NASCIMENTO,
        cpf: beneficiario.CPF,
        sexo: beneficiario.SEXO,
        plano: 127,
        planoValor: "659",
        nomeMae: beneficiario.NOME_MAE,
        MMYYYY1Pagamento: responsavelFinanceiro.MENSALIDADE,
      });
    }
    const options = {
      method: "POST",
      url: process.env.ODONTO_BENEFICIARIO_TOKEN,
      headers: {
        Authorization: `Bearer ${token}`,
      },
      data: {
        token: process.env.BENEFICIARIO_TOKEN,
        dados: {
                parcelaRetidaComissao: "",
                incluirMensalidades: "",
                parceiro: {
                           codigo: 71709,
                           tipoCobranca: 1,
                           adesionista: 0,
                           maxMensalidadeId: "",
                },
                responsavelFinanceiro: {
                  codigoContrato: responsavelFinanceiro.NATUREZA_CONTRATO,
                  nome: responsavelFinanceiro.NOME,
                  dataNascimento: responsavelFinanceiro.NASCIMENTO,
                  cpf: responsavelFinanceiro.CPF,
                  identidadeNumero: responsavelFinanceiro.RG,
                  identidadeOrgaoExpeditor: responsavelFinanceiro.ORGAO,
                  sexo: responsavelFinanceiro.SEXO,
                  tipoPagamento: 1,
                  origemVenda: 13,
                  departamento: idDepartamento,
                  matricula: "",
                  codSistemaExterno: "",
                  dataAssinaturaContrato: responsavelFinanceiro.INCLUSAO,
                  endereco: {
                      cep: responsavelFinanceiro.CEP,
                      tipoLogradouro: enderecoCep.IdTipoLogradouro,
                      logradouro: enderecoCep.Logradouro,
                      numero: responsavelFinanceiro.NUMERO_ENDERECO,
                      complemento: "",
                      bairro: enderecoCep.IdBairro,
                      municipio: enderecoCep.IdMunicipio,
                      uf: enderecoCep.IdUf,
                      descricaoUf: enderecoCep.IdUf + ' ' + enderecoCep.Uf,
                  },
                  contatoResponsavelFinanceiro: [
                      contatoResponsavelFinanceiro,
                  ],
              },
              dependente: dependentes, 
        },
      },
    };
    // Chamada à API externa
    const apiResponse = await axios.request(options);
    return apiResponse.data;
  } catch (error) {

    console.error("Erro ao incluir beneficiário:", error.message);
    throw error;
  }
};

// Inclui múltiplos beneficiários
exports.incluirVariosBeneficiarios = async (req, res) => {
  try {
    const { dados } = req.body;

    const resultado = await BeneficiarioOdonto.getBeneficiariosLista();
    const beneficiarios = Array.isArray(resultado.data) ? resultado.data : [];
    const inclusaoResultados = [];

    for (const beneficiario of beneficiarios) {
      try {
        const idBeneficiario = beneficiario.NNUMEUSUA;
        if (!idBeneficiario) {
          throw new Error('ID do beneficiário não encontrado.');
        }

        const resposta = await exports.incluirBeneficiario(idBeneficiario);
        inclusaoResultados.push({ beneficiario, status: 'success', resposta });
        if (resposta.resultMessage === 'Titular já cadastrado e ativo no contrato. ') {
          await LogService.saveLog(
            beneficiario.NOME,
            idBeneficiario,
            resposta.resultMessage,
            null
          );
        } else {
          if (resposta.resultMessage != '') {
            await LogService.saveLog(
              beneficiario.NOME,
              idBeneficiario,
              resposta.resultMessage,
              null
            );
          } else {
            await LogService.saveLog(
              beneficiario.NOME,
              idBeneficiario,
              'Beneficiário inserido com sucesso. ',
              null
            );            
          }
        }

      } catch (error) {
        await LogService.saveLog(
          beneficiario?.NOME || 'Nome não disponível',
          beneficiario?.NNUMEUSUA || 'ID não disponível',
          `Erro ao incluir beneficiário: ${error.message}`
        );
        inclusaoResultados.push({ beneficiario, status: 'error', error: error.message });
      }
    }

    return res.status(200).json({ message: 'Processamento concluído', inclusaoResultados });
  } catch (error) {
    console.error('Erro ao processar inclusão em massa:', error.message);
    return res.status(500).json({ message: 'Erro interno do servidor', error: error.message });
  }
};
