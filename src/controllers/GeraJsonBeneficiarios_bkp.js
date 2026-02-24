// src/controllers/GeraJsonBeneficiarios.js
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const knex = require('knex');
const axios = require('axios');
const { buscarCepOdonto } = require('./BuscaCepOdonto');

// carrega configuração do knexfile.js (production por padrão)
const knexConfig =
  require('../../knexfile')[process.env.NODE_ENV || 'production'];
const db = knex(knexConfig);

function log(msg) {
  console.log('[BENEF_JSON]', msg);
}

// pega linhas do db.raw independente do formato (knex + oracledb)
function extractRows(result) {
  if (!result) return [];
  if (Array.isArray(result) && !Array.isArray(result[0])) return result;
  if (Array.isArray(result.rows)) return result.rows;
  if (Array.isArray(result[0])) return result[0];
  return [];
}

// helper pra pegar campo sem se preocupar com maiúscula/minúscula
function get(row, ...names) {
  for (const n of names) {
    if (row[n] !== undefined) return row[n];
    const up = n.toUpperCase();
    const low = n.toLowerCase();
    if (row[up] !== undefined) return row[up];
    if (row[low] !== undefined) return row[low];
  }
  return null;
}

function onlyDigits(v) {
  return (v || '').toString().replace(/\D/g, '');
}

function trunc(v, len) {
  return (v || '').toString().substring(0, len);
}

// ================== LOG EM odonto_benef ==================

async function gravarOdontoBenef({
  nnumeusua,
  idOdonto,
  cpf,
  nome,
  statusEnvio,
  httpStatus,
  msgRetorno,
  jsonEnviado,
}) {
  try {
    const msg = (msgRetorno || '').toString().substring(0, 4000);
    const jsonStr = jsonEnviado ? JSON.stringify(jsonEnviado) : null;

    await db.raw(
      `
      INSERT INTO odonto_benef
        (id_log,
         nnumeusua,
         id_odonto,
         cpf,
         nome,
         dt_envio,
         status_envio,
         http_status,
         msg_retorno,
         json_enviado)
      VALUES
        (seq_odonto_benef.NEXTVAL,
         ?, ?, ?, ?, SYSDATE,
         ?, ?, ?, ?)
    `,
      [
        nnumeusua,
        idOdonto,
        cpf,
        nome,
        statusEnvio,
        httpStatus ? Number(httpStatus) : null,
        msg,
        jsonStr,
      ]
    );
  } catch (err) {
    log(
      `[ODONTO_BENEF] ERRO ao gravar log (nnumeusua=${nnumeusua}): ${err.message}`
    );
  }
}

// ============== TOKEN APIV3 =================

async function obterTokenApiv3() {
  const token = process.env.ODONTO_APIV3_TOKEN;

  if (!token) {
    throw new Error('ODONTO_APIV3_TOKEN não configurado no .env');
  }

  log('[INFO] Usando token APIV3 do .env (ODONTO_APIV3_TOKEN).');
  return token;
}

// ============== BUSCA TITULAR DIRETO NO BANCO =================

const SQL_TITULAR_POR_ID = `
SELECT
       '0'  AS parcelaRetidaComissao,
       '0'  AS incluirMensalidades,
       '71709' AS codigo,
       '1'  AS tipoCobranca,
       '0'  AS adesionista,
       '0'  AS maxMensalidadeId,
       '27543'                      AS codigoContrato,
       hssusua.cnomeusua           AS nome,
       TO_CHAR(hssusua.dnascusua,'YYYY-MM-DD') AS nascimento,
       hssusua.c_cpfusua           AS cpf,
       hssusua.c__rgusua           AS rg,
       NVL(hssusua.corrgusua, 'SSP') AS orgao,
       DECODE(hssusua.csexousua,'M','1','0') AS sexo,
       '13'                        AS origemVenda,
       odonto_depart.id_odonto      AS departamento,
       TO_CHAR(HSSTXUSU.dincltxusu,'YYYY-MM-DD') AS dataAssinaturaContrato,
       hsstitu.c_ceptitu           AS cep,
       hssendp.cnumeendp           as numero,
       '16'                        AS fl_AlteraSituacao,
       cont.tipo_contato,
       cont.contato,
       DECODE(hsstitu.cnatutitu,'3','27552','27552') AS natureza_contrato,
       hsstitu.nnumetitu,
       odonto_depart.cnpj          AS cnpj_empresa,
       hssempr.crazaempr          AS nome_empresa,
       TO_CHAR(HSSTXUSU.dincltxusu,'YYYY-MM-DD') AS inclusao,
       hssusua.ntituusua          AS titular,
       hsspess.nnumepess,
       TO_CHAR(HSSTXUSU.dincltxusu,'YYYY-MM-DD') AS dinclusua,
       hssusua.nnumeusua,
       hssusua.cnmaeusua          AS nome_mae,
       '1' AS tipo_usuario,
       TO_CHAR(HSSTXUSU.dincltxusu, 'YYYYMM') as MMYYYY1Pagamento
  FROM hssusua,
       hsstitu,
       hssempr,
       hsspess,
       hssendp,
       odonto_depart,
       hsstxusu,
       (
         SELECT nnumepess,
                contato,
                tipo_contato
           FROM (
                 SELECT c.nnumepess,
                        c.contato,
                        c.tipo_contato,
                        ROW_NUMBER() OVER (
                          PARTITION BY c.nnumepess
                          ORDER BY c.ordem
                        ) rn
                   FROM (
                         SELECT hssfonp.nnumepess,
                                hssfonp.cddd_fonp || hssfonp.cfonefonp AS contato,
                                CASE 
                                  WHEN hssfonp.ctipofonp = 'E' THEN '8'
                                  WHEN hssfonp.ctipofonp = 'R' THEN '1'
                                  WHEN hssfonp.ctipofonp = 'C' THEN '1'
                                  WHEN hssfonp.ctipofonp = 'W' THEN '10'
                                END AS tipo_contato,
                                1 AS ordem
                           FROM hssfonp
                          WHERE hssfonp.cprinfonp = 'S'

                         UNION ALL

                         SELECT hssemap.nnumepess,
                                hssemap.cmailemap AS contato,
                                '50' AS tipo_contato,
                                2    AS ordem
                           FROM hssemap
                          WHERE hssemap.cprinemap = 'S'
                        ) c
                )
          WHERE rn = 1
       ) cont
 WHERE 0 = 0
   AND hssusua.csituusua = 'A'
   AND hssusua.ctipousua = 'T'
   AND hssusua.nnumetitu = hsstitu.nnumetitu (+)
   AND hsstitu.nnumeempr = hssempr.nnumeempr (+)
   AND hssusua.nnumepess = hsspess.nnumepess (+)
   AND hsspess.nnumepess = hssendp.nnumepess(+)
   AND hssusua.nnumepess = cont.nnumepess(+)
   AND hssusua.nnumeusua = hsstxusu.nnumeusua (+)
   AND hssempr.c_cgcempr = odonto_depart.cnpj
   AND hssusua.nnumeusua = ?
`;

async function buscarTitularPorId(titularId) {
  if (!titularId) return null;
  try {
    const result = await db.raw(SQL_TITULAR_POR_ID, [titularId]);
    const rows = extractRows(result);
    if (!rows.length) {
      log(
        `[BENEF_JSON] Nenhum titular encontrado no banco para titularId=${titularId}`
      );
      return null;
    }
    log(
      `[BENEF_JSON] Titular encontrado no banco para titularId=${titularId}`
    );
    return rows[0];
  } catch (err) {
    log(
      `[BENEF_JSON] Erro ao buscar titularId=${titularId}: ${err.message}`
    );
    return null;
  }
}

// ============= ENVIO VIDA (AssociadoPJ) =================

async function enviarVidaOdonto(dadosVida, tokenApiv3, contexto) {
  const { nnumeusua, cpf, tipoUsuario, idOdonto, nome } = contexto;

  const baseApiV3 = process.env.ODONTO_BASE_APIV3; // ex: https://apiv3.odontogroup.com.br/api
  const s4eToken = process.env.ODONTO_S4E_TOKEN; // token fixo da S4E

  if (!baseApiV3 || !s4eToken) {
    throw new Error('Configure ODONTO_BASE_APIV3 e ODONTO_S4E_TOKEN no .env');
  }
  if (!tokenApiv3) {
    throw new Error('Token APIV3 não informado (ODONTO_APIV3_TOKEN vazio?).');
  }

  const url = `${baseApiV3}/AssociadoPJ?token=${s4eToken}`;

  log(
    `[BENEF_JSON] POST Vida: url=${url} nnumeusua=${nnumeusua} cpf=${cpf} tipoUsuario=${tipoUsuario}`
  );

  const payload = {
    token: s4eToken,
    dados: dadosVida,
  };

  try {
    const resp = await axios.post(url, payload, {
      headers: {
        Authorization: `Bearer ${tokenApiv3}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    });

    const data = resp.data || {};
    const bodyStr = JSON.stringify(data);

    log(
      `[BENEF_JSON] Vida enviada nnumeusua=${nnumeusua} status=${resp.status} body=${bodyStr.substring(
        0,
        500
      )}`
    );

    await gravarOdontoBenef({
      nnumeusua,
      idOdonto,
      cpf,
      nome,
      statusEnvio: 'OK_VIDA',
      httpStatus: resp.status,
      msgRetorno: data.resultMessage || data.message || bodyStr,
      jsonEnviado: payload,
    });

    return data;
  } catch (error) {
    const status = error.response?.status || 'no-response';
    const data = error.response?.data || {};
    const bodyStr = JSON.stringify(data);

    log(
      `[BENEF_JSON] ERRO envio vida: nnumeusua=${nnumeusua} status=${status} msg=${error.message} resp=${bodyStr}`
    );

    await gravarOdontoBenef({
      nnumeusua,
      idOdonto,
      cpf,
      nome,
      statusEnvio: 'ERRO_VIDA',
      httpStatus: status,
      msgRetorno: data.resultMessage || data.message || error.message,
      jsonEnviado: payload,
    });

    return data; // devolve pra chamador decidir se vai tentar NovoDependente
  }
}

// ============= GET associado-Emp (matricula_contrato_familia) =============

async function buscarMatriculaContratoFamilia(cpfTitular, tokenApiv3) {
  const baseApiV3 = process.env.ODONTO_BASE_APIV3;
  if (!baseApiV3) {
    throw new Error('ODONTO_BASE_APIV3 não configurado no .env');
  }

  const empresasParam = '[27543, 27552]';
  const url = `${baseApiV3}/associado-Emp?cpf=${cpfTitular}&empresas=${encodeURIComponent(
    empresasParam
  )}`;

  log(
    `[BENEF_JSON] GET associado-Emp: url=${url} cpf=${cpfTitular} empresas=${empresasParam}`
  );

  try {
    const resp = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${tokenApiv3}`,
      },
      timeout: 30000,
    });

    const data = resp.data;
    if (!Array.isArray(data) || !data.length) {
      log(
        `[BENEF_JSON] Nenhuma vida retornada em associado-Emp para cpf=${cpfTitular}`
      );
      return null;
    }

    const vida = data[0];
    const matricula = vida.matricula_contrato_familia;

    log(
      `[BENEF_JSON] matricula_contrato_familia=${matricula} obtido para cpf=${cpfTitular}`
    );
    return matricula;
  } catch (error) {
    const status = error.response?.status || 'no-response';
    const bodyStr = JSON.stringify(error.response?.data || {});

    log(
      `[BENEF_JSON] ERRO associado-Emp cpf=${cpfTitular} status=${status} msg=${error.message} resp=${bodyStr}`
    );
    return null;
  }
}

// ============= MONTAGEM DO JSON (AssociadoPJ) =============

function buildDadosFromGrupo(grupoRows, cepInfoTitular, titularRow) {
  if (!titularRow) {
    throw new Error('Grupo sem titular.');
  }

  const parcelaRetidaComissao = get(titularRow, 'parcelaRetidaComissao');
  const incluirMensalidades = get(titularRow, 'incluirMensalidades');

  const codigoParceiroRow = get(titularRow, 'codigo');
  let parceiroCodigo = Number(codigoParceiroRow || 72692);
  if (parceiroCodigo === 71709) {
    parceiroCodigo = 72692;
  }

  const tipoCobranca = get(titularRow, 'tipoCobranca');
  const adesionista = get(titularRow, 'adesionista');
  const maxMensalidadeId = get(titularRow, 'maxMensalidadeId');

  const codigoContrato =
    get(titularRow, 'natureza_contrato') || get(titularRow, 'codigoContrato');
  const nomeTitular = trunc(get(titularRow, 'nome'), 70);
  const nascimentoTit = get(titularRow, 'nascimento');
  const cpfTitular = onlyDigits(get(titularRow, 'cpf'));
  const rgTitular = get(titularRow, 'rg');
  const orgaoTitular = get(titularRow, 'orgao');
  const sexoTitular = Number(get(titularRow, 'sexo') || 1);
  const origemVenda = Number(get(titularRow, 'origemVenda') || 13);
  const departamento = String(get(titularRow, 'departamento') ?? '');

  const dataAssinatura = get(titularRow, 'dataAssinaturaContrato');
  const cepStrTitular = onlyDigits(get(titularRow, 'cep'));
  const fl_AlteraSituacao =
    Number(get(titularRow, 'fl_AlteraSituacao') || 16);

  const numeroEnderecoRaw = get(titularRow, 'numero');
  let numeroEndereco = (numeroEnderecoRaw || '').toString().trim();
  if (!numeroEndereco) {
    numeroEndereco = '0';
  }

  const contatoTitular = get(titularRow, 'contato');
  const tipo_contatoTitular = get(titularRow, 'tipo_contato');

  const inclusaoTitular =
    get(titularRow, 'inclusao') ||
    get(titularRow, 'dinclusua') ||
    dataAssinatura;

  const contatos = [];
  if (contatoTitular && tipo_contatoTitular) {
    contatos.push({
      tipo: Number(tipo_contatoTitular),
      dado: contatoTitular.toString(),
    });
  }

  const endereco = {
    cep: cepStrTitular || null,
    tipoLogradouro: cepInfoTitular
      ? Number(cepInfoTitular.IdTipoLogradouro)
      : null,
    logradouro: cepInfoTitular ? cepInfoTitular.Logradouro : '',
    numero: numeroEndereco,
    complemento: '',
    bairro: cepInfoTitular ? Number(cepInfoTitular.IdBairro) : null,
    municipio: cepInfoTitular ? Number(cepInfoTitular.IdMunicipio) : null,
    uf: cepInfoTitular ? Number(cepInfoTitular.IdUf) : null,
    descricaoUf: cepInfoTitular ? cepInfoTitular.Uf : '',
  };

  const dependente = grupoRows.map((row) => {
    const nome = trunc(get(row, 'nome'), 70);
    const nasc = get(row, 'nascimento');
    const cpf = onlyDigits(get(row, 'cpf'));
    const sexo = Number(get(row, 'sexo') || 1);
    const nomeMae = trunc(get(row, 'nome_mae'), 70);
    const tipo_user = Number(get(row, 'tipo_usuario') || 1);
    const inclusao =
      get(row, 'inclusao') || get(row, 'sysdate') || inclusaoTitular;
    const MMYYYY1Pagamento = get(row, 'MMYYYY1Pagamento');

    return {
      tipo: tipo_user,
      nome,
      dataNascimento: nasc,
      cpf,
      sexo,
      plano: 124,
      planoValor: '6.59',
      nomeMae,
      carenciaAtendimento: 1,
      MMYYYY1Pagamento,
      funcionarioCadastro: 72694,
      dataCadastroLoteContrato: inclusao,
    };
  });

  return {
    parcelaRetidaComissao: parcelaRetidaComissao ?? '0',
    incluirMensalidades: incluirMensalidades ?? '0',
    parceiro: {
      codigo: parceiroCodigo,
      tipoCobranca: Number(tipoCobranca || 1),
      adesionista: Number(adesionista || 0),
      maxMensalidadeId: String(maxMensalidadeId ?? '0'),
    },
    responsavelFinanceiro: {
      codigoContrato: Number(codigoContrato || 27543),
      nome: nomeTitular,
      dataNascimento: nascimentoTit,
      cpf: cpfTitular,
      sexo: sexoTitular,
      identidadeNumero: rgTitular,
      identidadeOrgaoExpeditor: orgaoTitular,
      matricula: `MAT-${cpfTitular}`,
      dataApresentacao: inclusaoTitular,
      diaVencimento: '01',
      tipoPagamento: 513,
      origemVenda,
      departamento,
      dataAssinaturaContrato: inclusaoTitular || dataAssinatura,
      endereco,
      fl_AlteraSituacao,
      contatoResponsavelFinanceiro: contatos,
    },
    dependente,
  };
}

// ============= MONTAGEM JSON NovoDependente =============

function buildDadosNovoDependente(
  dependenteRow,
  titularRow,
  matriculaContratoFamilia
) {
  const codigoParceiroRow = get(titularRow, 'codigo');
  let parceiroCodigo = Number(codigoParceiroRow || 72692);
  if (parceiroCodigo === 71709) parceiroCodigo = 72692;

  const adesionista = Number(get(titularRow, 'adesionista') || 0);

  const dataAssinaturaContrato =
    get(titularRow, 'dataAssinaturaContrato') ||
    get(titularRow, 'inclusao') ||
    get(titularRow, 'sysdate');

  const tipo = Number(get(dependenteRow, 'tipo_usuario') || 1);
  const nome = trunc(get(dependenteRow, 'nome'), 70);
  const cpf = onlyDigits(get(dependenteRow, 'cpf'));
  const sexo = Number(get(dependenteRow, 'sexo') || 1);
  const nomeMae = trunc(get(dependenteRow, 'nome_mae'), 70);
  const nasc = get(dependenteRow, 'nascimento');
  const inclusaoDep =
    get(dependenteRow, 'inclusao') ||
    get(dependenteRow, 'sysdate') ||
    dataAssinaturaContrato;
  const MMYYYY1Pagamento = get(dependenteRow, 'MMYYYY1Pagamento');

  return {
    parceiro: {
      codigo: parceiroCodigo,
      adesionista,
    },
    responsavelFinanceiro: {
      codigo: Number(matriculaContratoFamilia),
      dataAssinaturaContrato: dataAssinaturaContrato || inclusaoDep,
    },
    dependente: [
      {
        tipo,
        nome,
        dataNascimento: nasc,
        cpf,
        sexo,
        plano: 124,
        planoValor: '6.59',
        nomeMae,
        carenciaAtendimento: 1,
        MMYYYY1Pagamento,
        funcionarioCadastro: 72694,
        dataCadastroLoteContrato: inclusaoDep,
        numeroProposta: '',
        rcaId: 0,
        cd_orientacao_sexual: 0,
        OutraOrientacaoSexual: '',
        cd_ident_genero: 0,
        OutraIdentidadeGenero: '',
        idExterno: '',
        numeroCarteira: '',
        observacaoUsuario: '',
      },
    ],
    contatoDependente: [],
  };
}

// ============= ENVIO NovoDependente (S4E) =============

async function enviarNovoDependente(
  dependenteRow,
  titularRow,
  matriculaContratoFamilia,
  tokenApiv3 // mantido se precisar no futuro
) {
  const nnumeusua = get(dependenteRow, 'nnumeusua');
  const idOdonto = Number(get(titularRow, 'departamento'));
  const nomeDep = trunc(get(dependenteRow, 'nome'), 100);
  const cpfDep = onlyDigits(get(dependenteRow, 'cpf'));
  const tipoUsuario = Number(get(dependenteRow, 'tipo_usuario') || 1);

  const s4eToken = process.env.ODONTO_S4E_TOKEN;
  const baseRaw =
    process.env.ODONTO_S4E_BASE || 'https://odontogroup.s4e.com.br';

  if (!s4eToken) {
    throw new Error('ODONTO_S4E_TOKEN não configurado no .env');
  }

  const s4eBase = baseRaw.replace(/\/+$/, '');
  const url = `${s4eBase}/api/vendedor/NovoDependente?token=${s4eToken}`;

  const dados = buildDadosNovoDependente(
    dependenteRow,
    titularRow,
    matriculaContratoFamilia
  );

  const payload = { token: s4eToken, dados };

  log(
    `[BENEF_JSON] POST NovoDependente: url=${url} nnumeusua=${nnumeusua} cpf=${cpfDep} tipoUsuario=${tipoUsuario}`
  );

  try {
    const resp = await axios.post(url, payload, {
      timeout: 30000,
    });

    const data = resp.data || {};
    const bodyStr = JSON.stringify(data);

    log(
      `[BENEF_JSON] NovoDependente enviado nnumeusua=${nnumeusua} status=${resp.status} body=${bodyStr.substring(
        0,
        500
      )}`
    );

    await gravarOdontoBenef({
      nnumeusua,
      idOdonto,
      cpf: cpfDep,
      nome: nomeDep,
      statusEnvio: 'OK_DEP',
      httpStatus: resp.status,
      msgRetorno: data.mensagem || data.message || bodyStr,
      jsonEnviado: payload,
    });

    return { data, payload };
  } catch (error) {
    const status = error.response?.status || 'no-response';
    const data = error.response?.data || {};
    const bodyStr = JSON.stringify(data);

    log(
      `[BENEF_JSON] ERRO NovoDependente nnumeusua=${nnumeusua} status=${status} msg=${error.message} resp=${bodyStr}`
    );

    await gravarOdontoBenef({
      nnumeusua,
      idOdonto,
      cpf: cpfDep,
      nome: nomeDep,
      statusEnvio: 'ERRO_DEP',
      httpStatus: status,
      msgRetorno: data.mensagem || data.message || error.message,
      jsonEnviado: payload,
    });

    return { data: null, payload };
  }
}

// ============= ARQUIVO DE LOG =============

function buildFilePath() {
  const now = new Date();
  const year = String(now.getFullYear());
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const time =
    String(now.getHours()).padStart(2, '0') +
    String(now.getMinutes()).padStart(2, '0') +
    String(now.getSeconds()).padStart(2, '0');

  const dir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'beneficiario',
    year,
    month,
    day
  );
  fs.mkdirSync(dir, { recursive: true });

  const file = `${time}_beneficiarios.json`;
  return path.join(dir, file);
}

// ============= SQL PRINCIPAL (ENXUTO) =============

const SQL_BENEFICIARIOS = `
WITH registros_unicos AS (
  SELECT
         -- dados da pessoa
         hssusua.cnomeusua                               AS nome,
         TO_CHAR(hssusua.dnascusua,'YYYY-MM-DD')         AS nascimento,
         hssusua.c_cpfusua                               AS cpf,
         hssusua.c__rgusua                               AS rg,
         NVL(hssusua.corrgusua, 'SSP')                   AS orgao,
         DECODE(hssusua.csexousua,'M','1','0')           AS sexo,

         -- contrato / empresa / endereço
         odonto_depart.id_odonto                         AS departamento,
         TO_CHAR(sysdate,'YYYY-MM-DD')       AS dataAssinaturaContrato,
         hsstitu.c_ceptitu                               AS cep,
         CASE
           WHEN REGEXP_LIKE(TRIM(hssendp.cnumeendp), '^\\d+$')
           THEN TO_NUMBER(TRIM(hssendp.cnumeendp))
           ELSE 0
         END AS numero,
         cont.tipo_contato,
         cont.contato,
         DECODE(hsstitu.cnatutitu,'3','27552','27552')   AS natureza_contrato,
         hsstitu.nnumetitu,
         odonto_depart.cnpj                              AS cnpj_empresa,
         hssempr.crazaempr                               AS nome_empresa,
         TO_CHAR(sysdate,'YYYY-MM-DD')       AS inclusao,
         hssusua.ntituusua                               AS titular,
         hsspess.nnumepess,
         TO_CHAR(sysdate,'YYYY-MM-DD')       AS dinclusua,
         hssusua.nnumeusua,
         hssusua.cnmaeusua                               AS nome_mae,

         -- tipo de usuário
         CASE 
           WHEN hssusua.ctipousua = 'T' THEN '1'
           WHEN hssusua.ctipousua = 'D' THEN
             CASE 
               WHEN hssusua.cgrauusua = 'F' THEN '4'
               WHEN hssusua.cgrauusua = 'E' THEN '3'
               WHEN hssusua.cgrauusua = 'J' THEN '3'
               WHEN hssusua.cgrauusua = 'H' THEN '6'
               WHEN hssusua.cgrauusua = 'P' THEN '8'
               WHEN hssusua.cgrauusua = 'M' THEN '8'
               WHEN hssusua.cgrauusua = 'D' THEN '10'
               WHEN hssusua.cgrauusua = 'O' THEN '10'
               ELSE '10'
             END
           ELSE '10'
         END AS tipo_usuario,

         -- mês/ano 1º pagamento
         TO_CHAR(sysdate, 'YYYYMM')          AS MMYYYY1Pagamento,

         ROW_NUMBER() OVER (
           PARTITION BY hssusua.nnumeusua
           ORDER BY hssusua.dinclusua DESC
         ) AS rn
    FROM hssusua,
         hsstitu,
         hssempr,
         hsspess,
         hssendp,
         hsstxusu,
         odonto_depart,
         (
           SELECT nnumepess,
                  contato,
                  tipo_contato
             FROM (
                   SELECT c.nnumepess,
                          c.contato,
                          c.tipo_contato,
                          ROW_NUMBER() OVER (
                            PARTITION BY c.nnumepess
                            ORDER BY c.ordem
                          ) rn
                     FROM (
                           SELECT hssfonp.nnumepess,
                                  hssfonp.cddd_fonp || hssfonp.cfonefonp AS contato,
                                  CASE 
                                    WHEN hssfonp.ctipofonp = 'E' THEN '8'
                                    WHEN hssfonp.ctipofonp = 'R' THEN '1'
                                    WHEN hssfonp.ctipofonp = 'C' THEN '1'
                                    WHEN hssfonp.ctipofonp = 'W' THEN '10'
                                  END AS tipo_contato,
                                  1 AS ordem
                             FROM hssfonp
                            WHERE hssfonp.cprinfonp = 'S'

                           UNION ALL

                           SELECT hssemap.nnumepess,
                                  hssemap.cmailemap AS contato,
                                  '50' AS tipo_contato,
                                  2    AS ordem
                             FROM hssemap
                            WHERE hssemap.cprinemap = 'S'
                         ) c
                 )
            WHERE rn = 1
         ) cont
   WHERE 0 = 0
     AND hsstxusu.dcanctxusu IS NULL
     AND hsstxusu.nnumetxmen IN (
           155616900,
           163198861,
           168980581,
           141287944,
           208002771
         )
     AND hssusua.csituusua = 'A'
     AND hssusua.nnumetitu = hsstitu.nnumetitu (+)
     AND hsstitu.nnumeempr = hssempr.nnumeempr (+)
     AND hssusua.nnumepess = hsspess.nnumepess (+)
     AND hsspess.nnumepess = hssendp.nnumepess(+)
     AND hssusua.nnumeusua = hsstxusu.nnumeusua (+)
     AND hssusua.nnumepess = cont.nnumepess(+)
     AND hssempr.c_cgcempr = odonto_depart.cnpj
     --AND hssusua.nnumeusua not IN (select NNUMEUSUA
                                 --    from odonto_associado_emp_chk
                                  --  where status_api = 'ENCONTRADO')
                                  --  )
     AND hssusua.c_cpfusua in ('05137141976') )
SELECT *
  FROM registros_unicos
 WHERE rn = 1
`;

// ============= FUNÇÃO PRINCIPAL =============

async function gerarJsonBeneficiarios() {
  let tokenApiv3 = null;

  try {
    log('Executando SELECT de beneficiários (TODOS do dia anterior)...');
    const result = await db.raw(SQL_BENEFICIARIOS);
    const rows = extractRows(result);
    log(`Linhas retornadas: ${rows.length}`);

    if (!rows.length) {
      log('Nenhum registro encontrado. Nada a salvar.');
      return;
    }

    tokenApiv3 = await obterTokenApiv3();
    const s4eToken = process.env.ODONTO_S4E_TOKEN || 'TOKEN_NAO_CONFIGURADO';

    // agrupa por titular
    const grupos = new Map();
    for (const row of rows) {
      const titularId = get(row, 'titular') || get(row, 'nnumeusua');
      if (!grupos.has(titularId)) {
        grupos.set(titularId, []);
      }
      grupos.get(titularId).push(row);
    }

    const listaPayloads = [];

    for (const [titularId, grupoRows] of grupos.entries()) {
      // tenta achar titular no grupo
      let titularRow = grupoRows.find(
        (r) => Number(get(r, 'tipo_usuario')) === 1
      );

      if (!titularRow) {
        log(
          `[BENEF_JSON] Grupo titular=${titularId} sem tipo_usuario=1 no SELECT principal, buscando titular no banco...`
        );
        const titularRowDb = await buscarTitularPorId(titularId);
        if (!titularRowDb) {
          log(
            `[BENEF_JSON] Grupo titular=${titularId} continua sem titular (nem no banco). PULANDO.`
          );
          continue;
        }
        titularRow = titularRowDb;
        grupoRows.unshift(titularRow);
      }

      const cpfTitular = onlyDigits(get(titularRow, 'cpf'));
      const nomeTitular = trunc(get(titularRow, 'nome'), 100);
      const idOdonto = Number(get(titularRow, 'departamento'));
      const nnumeusuaTitular = get(titularRow, 'nnumeusua');

      const cepTitular = onlyDigits(get(titularRow, 'cep'));
      const cepInfoTitular = await buscarCepOdonto(cepTitular);

      log(
        `[BENEF_JSON] Montando/enviando JSON grupo titular=${titularId} cpf=${cpfTitular} qtdVidas=${grupoRows.length} id_odonto=${idOdonto}`
      );

      // 1) tenta envio normal (AssociadoPJ)
      const dadosVida = buildDadosFromGrupo(
        grupoRows,
        cepInfoTitular,
        titularRow
      );
      const payloadVida = { token: s4eToken, dados: dadosVida };
      listaPayloads.push({
        tipoEnvio: 'AssociadoPJ',
        titularId,
        payload: payloadVida,
      });

      const respVida = await enviarVidaOdonto(dadosVida, tokenApiv3, {
        nnumeusua: nnumeusuaTitular,
        cpf: cpfTitular,
        tipoUsuario: 1,
        idOdonto,
        nome: nomeTitular,
      });

      const msgVida = (respVida && respVida.resultMessage) || '';
      const codigoVida = respVida && respVida.resultCode;

      const titularJaCadastrado =
        msgVida.includes('Titular já cadastrado') || codigoVida === 3;

      if (!titularJaCadastrado) {
        // incluiu tudo OK ou deu outro erro – não tenta NovoDependente
        continue;
      }

      log(
        `[BENEF_JSON] Titular já cadastrado no contrato (grupo titular=${titularId}). Enviando apenas dependentes via NovoDependente...`
      );

      // 2) busca matricula_contrato_familia
      const matriculaContratoFamilia = await buscarMatriculaContratoFamilia(
        cpfTitular,
        tokenApiv3
      );

      if (!matriculaContratoFamilia) {
        log(
          `[BENEF_JSON] Não foi possível obter matricula_contrato_familia para cpf=${cpfTitular}. Dependentes não enviados.`
        );
        continue;
      }

      // 3) envia cada dependente (tipo_usuario != 1)
      const dependentes = grupoRows.filter(
        (r) => Number(get(r, 'tipo_usuario')) !== 1
      );

      if (!dependentes.length) {
        log(
          `[BENEF_JSON] Grupo titular=${titularId} não possui dependentes para NovoDependente.`
        );
        continue;
      }

      for (const depRow of dependentes) {
        const { payload } = await enviarNovoDependente(
          depRow,
          titularRow,
          matriculaContratoFamilia,
          tokenApiv3
        );

        listaPayloads.push({
          tipoEnvio: 'NovoDependente',
          titularId,
          payload,
        });
      }
    }

    if (!listaPayloads.length) {
      log('Nenhum payload gerado para log em arquivo.');
      return;
    }

    const filePath = buildFilePath();
    fs.writeFileSync(
      filePath,
      JSON.stringify(listaPayloads, null, 2),
      'utf8'
    );
    log(`Arquivo de log gerado: ${filePath}`);
  } catch (err) {
    console.error('[BENEF_JSON] Erro geral:', err.message || err);
  } finally {
    if (db && typeof db.destroy === 'function') {
      await db.destroy();
    }
  }
}

module.exports = { gerarJsonBeneficiarios };

if (require.main === module) {
  gerarJsonBeneficiarios();
}
