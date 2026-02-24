// src/controllers/GeraApenasJsonBeneficiarios.js
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const knex = require('knex');
const { buscarCepOdonto } = require('./BuscaCepOdonto');

// carrega configuração do knexfile.js (production por padrão)
const knexConfig =
  require('../../knexfile')[process.env.NODE_ENV || 'production'];
const db = knex(knexConfig);

function log(msg) {
  console.log('[BENEF_JSON_PREVIEW]', msg);
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

// ================= BUSCA TITULAR DIRETO NO BANCO =================

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
       TO_CHAR(SYSDATE,'YYYY-MM-DD') AS dataAssinaturaContrato,
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
        `[BENEF_JSON_PREVIEW] Nenhum titular encontrado no banco para titularId=${titularId}`
      );
      return null;
    }
    log(
      `[BENEF_JSON_PREVIEW] Titular encontrado no banco para titularId=${titularId}`
    );
    return rows[0];
  } catch (err) {
    log(
      `[BENEF_JSON_PREVIEW] Erro ao buscar titularId=${titularId}: ${err.message}`
    );
    return null;
  }
}

// ================= MONTAGEM DO JSON POR GRUPO (TITULAR + DEPENDENTES) =================

function buildDadosFromGrupo(grupoRows, cepInfoTitular, titularRow) {
  if (!titularRow) {
    throw new Error('Grupo sem titular (tipo_usuario = 1).');
  }

  const parcelaRetidaComissao = get(titularRow, 'parcelaRetidaComissao');
  const incluirMensalidades   = get(titularRow, 'incluirMensalidades');

  // código parceiro: se vier 71709 (homolog), força 72692 (produção)
  const codigoParceiroRow = get(titularRow, 'codigo'); // 71709 no SELECT
  let parceiroCodigo = Number(codigoParceiroRow || 72692);
  if (parceiroCodigo === 71709) {
    parceiroCodigo = 72692;
  }

  const tipoCobranca         = get(titularRow, 'tipoCobranca'); // 1
  const adesionista          = get(titularRow, 'adesionista');  // 0
  const maxMensalidadeId     = get(titularRow, 'maxMensalidadeId');

  const codigoContrato   =
    get(titularRow, 'natureza_contrato') || get(titularRow, 'codigoContrato');
  const nomeTitular      = trunc(get(titularRow, 'nome'), 70);
  const nascimentoTit    = get(titularRow, 'nascimento'); // "YYYY-MM-DD"
  const cpfTitular       = onlyDigits(get(titularRow, 'cpf'));
  const rgTitular        = get(titularRow, 'rg');
  const orgaoTitular     = get(titularRow, 'orgao'); // o que veio do SELECT
  const sexoTitular      = Number(get(titularRow, 'sexo') || 1);
  const origemVenda      = Number(get(titularRow, 'origemVenda') || 13);

  // departamento = id_odonto (pro JSON) e também vai pro log
  const departamento     = String(get(titularRow, 'departamento') ?? '');

  const dataAssinatura   = get(titularRow, 'dataAssinaturaContrato');
  const cepStrTitular    = onlyDigits(get(titularRow, 'cep'));
  const fl_AlteraSituacao =
    Number(get(titularRow, 'fl_AlteraSituacao') || 16);

  const numeroEnderecoRaw = get(titularRow, 'numero');
  let numeroEndereco = (numeroEnderecoRaw || '').toString().trim();
  if (!numeroEndereco) {
    // API não aceita vazio -> manda algo padrão
    numeroEndereco = '0';
  }

  const contatoTitular      = get(titularRow, 'contato');
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

  // monta a lista de TODAS as vidas (titular + dependentes)
  const dependente = grupoRows.map((row) => {
    const nome      = trunc(get(row, 'nome'), 70);
    const nasc      = get(row, 'nascimento');
    const cpf       = onlyDigits(get(row, 'cpf'));
    const sexo      = Number(get(row, 'sexo') || 1);
    const nomeMae   = trunc(get(row, 'nome_mae'), 70);
    const tipo_user = Number(get(row, 'tipo_usuario') || 1);
    const inclusao  =
      get(row, 'inclusao') || get(row, 'dinclusua') || inclusaoTitular;
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
      funcionarioCadastro: 72694, // PRODUÇÃO
      dataCadastroLoteContrato: inclusao,
    };
  });

  const dados = {
    parcelaRetidaComissao: parcelaRetidaComissao ?? '0',
    incluirMensalidades: incluirMensalidades ?? '0',
    parceiro: {
      codigo: parceiroCodigo, // PRODUÇÃO
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

  return dados;
}

function buildFilePath() {
  const now = new Date();
  const year = String(now.getFullYear());
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const time =
    String(now.getHours()).toString().padStart(2, '0') +
    String(now.getMinutes()).toString().padStart(2, '0') +
    String(now.getSeconds()).toString().padStart(2, '0');

  const dir = path.join(
    __dirname,
    '..',
    '..',
    'logs',
    'odonto',
    'beneficiario_preview',
    year,
    month,
    day
  );
  fs.mkdirSync(dir, { recursive: true });

  const file = `${time}_beneficiarios_preview.json`;
  return path.join(dir, file);
}

// MESMO SELECT que você mandou, já com MMYYYY1Pagamento
const SQL_BENEFICIARIOS = `
SELECT *
  FROM (
         WITH registros_unicos AS (
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
                  TO_CHAR(HSSTXUSU.dincltxusu, 'YYYYMM') as MMYYYY1Pagamento , 
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
              AND HSSUSUA.nnumeusua IN (621034) 
              --AND HSSTXUSU.dincltxusu = '22/12/2025'
         )
         SELECT *
           FROM registros_unicos
          WHERE rn = 1
       )
     
`;

// função principal – NÃO ENVIA, só grava o JSON
async function gerarApenasJsonBeneficiarios() {
  try {
    log('Executando SELECT de beneficiários (APENAS JSON, sem envio)...');
    const result = await db.raw(SQL_BENEFICIARIOS);
    const rows = extractRows(result);
    log(`Linhas retornadas: ${rows.length}`);

    if (!rows.length) {
      log('Nenhum registro encontrado. Nada a salvar.');
      return;
    }

    const s4eToken = process.env.ODONTO_S4E_TOKEN || 'TOKEN_NAO_CONFIGURADO';

    // 1) agrupa por TITULAR (ntituusua); se não tiver, usa o próprio nnumeusua
    const grupos = new Map();
    for (const row of rows) {
      const titularId = get(row, 'titular') || get(row, 'nnumeusua');

      if (!grupos.has(titularId)) {
        grupos.set(titularId, []);
      }
      grupos.get(titularId).push(row);
    }

    const lista = [];

    // 2) para cada grupo (contrato) monta UM JSON
    for (const [titularId, grupoRows] of grupos.entries()) {
      // tenta achar titular no próprio grupo
      let titularRow = grupoRows.find(
        (r) => Number(get(r, 'tipo_usuario')) === 1
      );

      // se não tiver titular no grupo, busca no banco
      if (!titularRow) {
        log(
          `[BENEF_JSON_PREVIEW] Grupo titular=${titularId} sem tipo_usuario=1 no SELECT principal, buscando titular no banco...`
        );
        const titularRowDb = await buscarTitularPorId(titularId);
        if (!titularRowDb) {
          log(
            `[BENEF_JSON_PREVIEW] Grupo titular=${titularId} continua sem titular (nem no banco). JSON NÃO GERADO.`
          );
          continue;
        }
        titularRow = titularRowDb;

        // adiciona o titular buscado ao grupo, pra entrar no array de dependentes
        grupoRows.unshift(titularRow);
      }

      const cpfTitular = onlyDigits(get(titularRow, 'cpf'));
      const nomeTitular = trunc(get(titularRow, 'nome'), 100);
      const idOdonto = Number(get(titularRow, 'departamento'));

      const cepTitular = onlyDigits(get(titularRow, 'cep'));
      const cepInfoTitular = await buscarCepOdonto(cepTitular);

      log(
        `[BENEF_JSON_PREVIEW] Montando JSON grupo titular=${titularId} cpf=${cpfTitular} ` +
          `qtdVidas=${grupoRows.length} id_odonto=${idOdonto}`
      );

      const dados = buildDadosFromGrupo(grupoRows, cepInfoTitular, titularRow);

      // mesmo formato do body que iria para API
      lista.push({ token: s4eToken, dados });
    }

    if (!lista.length) {
      log('Nenhum grupo com titular válido para gerar JSON.');
      return;
    }

    const filePath = buildFilePath();
    fs.writeFileSync(filePath, JSON.stringify(lista, null, 2), 'utf8');
    log(`Arquivo de PREVIEW gerado (apenas JSON, sem envio): ${filePath}`);
  } catch (err) {
    console.error('[BENEF_JSON_PREVIEW] Erro geral:', err.message || err);
  } finally {
    if (db && typeof db.destroy === 'function') {
      await db.destroy();
    }
  }
}

module.exports = { gerarApenasJsonBeneficiarios };

if (require.main === module) {
  gerarApenasJsonBeneficiarios();
}
