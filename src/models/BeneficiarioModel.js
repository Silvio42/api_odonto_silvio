const db = require('../database/dbConnection');

class BeneficiarioOdonto {
  // Método para construir a query SQL dinamicamente
  static buildQuery(isTitular) {
    let query = `
      SELECT
          hssusua.cnomeusua AS nome,
          hssusua.c_cpfusua AS cpf,
          TO_CHAR(hssusua.dnascusua, 'YYYY-MM-DD') AS nascimento,
          hssusua.c__rgusua AS rg,
          NVL(hssusua.corrgusua, 'SSP') AS orgao,
          DECODE(hssusua.csexousua, 'M', '1', '0') AS sexo,
          DECODE(hsstitu.cnatutitu, '3', '27552', '27552') AS natureza_contrato,
          hsstitu.nnumetitu,
          hssempr.c_cgcempr AS cnpj_empresa,
          hssempr.crazaempr AS nome_empresa,
          hsstitu.c_ceptitu AS cep,
          TO_CHAR(hsstitu.dconttitu, 'YYYY-MM-DD') AS inclusao,
          hsstitu.cnumetitu AS numero_endereco,
          hssusua.ntituusua AS titular,
          hsspess.nnumepess,
          TO_CHAR(hssusua.dinclusua, 'YYYY-MM-DD') AS dinclusua,
          hssusua.nnumeusua,
          hssusua.cnmaeusua AS nome_mae,
          CASE 
              WHEN hssusua.ctipousua = 'T' THEN '1' -- Titular
              WHEN hssusua.ctipousua = 'D' THEN
                  CASE 
                      WHEN hssusua.cgrauusua = 'F' THEN '4' -- Dependente e Filho
                      WHEN hssusua.cgrauusua = 'E' THEN '3' -- Dependente e Esposa
                      WHEN hssusua.cgrauusua = 'J' THEN '3' -- Dependente e Conjuge
                      WHEN hssusua.cgrauusua = 'H' THEN '6' -- Dependente e Enteado
                      WHEN hssusua.cgrauusua = 'P' THEN '8' -- Dependente e Pai
                      WHEN hssusua.cgrauusua = 'M' THEN '8' -- Dependente e Mãe
                      WHEN hssusua.cgrauusua = 'D' THEN '10' -- Dependente
                      WHEN hssusua.cgrauusua = 'O' THEN '10' -- Outro
                  ELSE '10' -- Caso não se enquadre em nenhuma categoria
                  END
              ELSE '10' -- Caso não seja titular ou dependente
          END AS tipo_usuario
      FROM
          hssusua, hsstitu, hssempr, hsspess, hssendp`;

    if (!isTitular) {
      query += `, hsstxusu`;
    }

    query += `
      WHERE
          hssusua.nnumeusua = ?
          AND hssusua.nnumetitu = hsstitu.nnumetitu
          AND hsstitu.nnumeempr = hssempr.nnumeempr
          AND hssusua.nnumepess = hsspess.nnumepess
          AND hsspess.nnumepess = hssendp.nnumepess (+)`;

    // Adiciona condições extras apenas se não for titular
    if (!isTitular) {
      query += `
          AND hsstxusu.dcanctxusu IS NULL
          AND hsstxusu.nnumetxmen IN (141287944, 155616900, 163198861, 168980581)
          AND hssusua.nnumeusua = hsstxusu.nnumeusua`;
    }

    return query;
  }

  // Método: buscar dados de Beneficiário (dinâmico)
  static async getBeneficiario(idBeneficiario, isTitular) {
    try {
      const query = this.buildQuery(isTitular);
      const result = await db.raw(query, [idBeneficiario]);

      if (result.length === 0) {
        return { error: false, data: null };
      }

      return { error: false, data: result[0] };
    } catch (error) {
      console.error(error);
      return { error: true, message: error.message };
    }
  }

  static async getBeneficiarioTitular(idBeneficiario) {
    return this.getBeneficiario(idBeneficiario, true);
  }

  static async getBeneficiarioAditivo(idBeneficiario) {
    return this.getBeneficiario(idBeneficiario, false);
  }

  static async getBeneficiarioContato(idBeneficiario) {
    const data = await db.raw(
      `select hssfonp.cddd_fonp||hssfonp.cfonefonp contato,
         case 
           when hssfonp.ctipofonp = 'E' then '8' --celular
           when hssfonp.ctipofonp = 'R' then '1' --Residencial  
           when hssfonp.ctipofonp = 'C' then '1' --Comercial  
           when hssfonp.ctipofonp = 'W' then '10'--Whatsapp                                  
         end as tipo_contato
         from hssfonp, hssusua
        where hssusua.nnumeusua = ?
          and hssfonp.cprinfonp = 'S'
          and hssusua.nnumepess = hssfonp.nnumepess (+)

       union all

       select hssemap.cmailemap contato, '50'
         from hssemap, hssusua
        where hssusua.nnumeusua = ?
          and hssemap.cprinemap = 'S'
          and hssusua.nnumepess = hssemap.nnumepess (+)`, [idBeneficiario, idBeneficiario]
    )
      .then(result => {

        if (result.length > 0) {
          return { error: false, data: result }
        } else {
          return { error: false, data: null }
        }
      })
      .catch(error => {
        console.log(error);
        return { error: true, data: null }
      });

    return data
  }

  static async getBeneficiariosLista() {
    
    const data = await db.raw(
      `select * from (
       WITH registros_unicos AS (
        select hssusua.cnomeusua nome, hssusua.c_cpfusua cpf, TO_CHAR(hssusua.dnascusua,'YYYY-MM-DD') nascimento, hssusua.c__rgusua rg,
              nvl(hssusua.corrgusua, 'SSP') orgao, decode(hssusua.csexousua,'M','1','0') sexo, decode(hsstitu.cnatutitu,'3','27552','27552') natureza_contrato,
              hsstitu.nnumetitu, hssempr.c_cgcempr cnpj_empresa, hssempr.crazaempr nome_empresa, hsstitu.c_ceptitu cep,
              TO_CHAR(hsstitu.dconttitu,'YYYY-MM-DD') inclusao, hsstitu.cnumetitu numero_endereco,
              hssusua.ntituusua titular,hsspess.nnumepess, TO_CHAR(hssusua.dinclusua, 'YYYY-MM-DD') dinclusua, hssusua.nnumeusua, hssusua.cnmaeusua nome_mae,
              CASE 
                WHEN hssusua.ctipousua = 'T' THEN '1' -- Titular
                WHEN hssusua.ctipousua = 'D' THEN
                  CASE 
                    WHEN hssusua.cgrauusua = 'F' THEN '4' -- Dependente e Filho
                    WHEN hssusua.cgrauusua = 'E' THEN '3' -- Dependente e Esposa
                    WHEN hssusua.cgrauusua = 'J' THEN '3' -- Dependente e Conjuge
                    WHEN hssusua.cgrauusua = 'H' THEN '6' -- Dependente e Enteado
                    WHEN hssusua.cgrauusua = 'P' THEN '8' -- Dependente e Pai
                    WHEN hssusua.cgrauusua = 'M' THEN '8' -- Dependente e Mãe
                    WHEN hssusua.cgrauusua = 'D' THEN '10' -- Dependente
                    WHEN hssusua.cgrauusua = 'O' THEN '10' -- Outro
                  ELSE '10' -- Caso não se enquadre em nenhuma categoria
                   END
               ELSE '10' -- Caso não seja titular ou dependente
               END AS tipo_usuario, 'importado' importado,
              ROW_NUMBER() OVER (PARTITION BY hssusua.c_cpfusua, hssempr.c_cgcempr ORDER BY hssusua.dinclusua DESC) AS rn
         from hssusua,hsstitu,hssempr, hsspess, hssendp, hsstxusu 
        where 0=0 
          and hsstxusu.dcanctxusu is null
          and hsstxusu.nnumetxmen in(141287944, 155616900, 163198861, 168980581)
          --and hssusua.nnumeplan in (17465752,17516920,144533458,144534292,108651267,
                                    --108651616,108665906,108651878,24625418,110784958,8178046)
          and hssusua.csituusua = 'A'
          and hssusua.nnumetitu = hsstitu.nnumetitu
          and hsstitu.nnumeempr = hssempr.nnumeempr
          and hssusua.nnumepess = hsspess.nnumepess
          and hsspess.nnumepess = hssendp.nnumepess (+)
          and hssusua.nnumeusua = hsstxusu.nnumeusua
        )
        SELECT *
          FROM registros_unicos
         WHERE rn = 1
    ) WHERE ROWNUM <= 16`)
      .then(result => {
        if (!result.length === false) {
          return { error: false, data: result }
        } else {
          return { error: false, data: null }
        }
      })
      .catch(error => {
        console.log(error);
        return { error: true, data: null }
      });
    return data
  }

}

module.exports = BeneficiarioOdonto;
