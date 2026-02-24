CREATE TABLE odonto_benef (
  id_log        NUMBER        NOT NULL,     -- chave técnica (log)
  nnumeusua     NUMBER        NOT NULL,     -- ID Solus
  id_odonto     NUMBER,                    -- ID Odonto
  cpf           VARCHAR2(11),
  nome          VARCHAR2(100),
  dt_envio      DATE DEFAULT SYSDATE,      -- quando tentou enviar
  status_envio  VARCHAR2(20),              -- OK / ERRO / PENDENTE
  http_status   NUMBER,                    -- 200, 400, 403, 500...
  msg_retorno   VARCHAR2(4000),            -- texto da API
  json_enviado  CLOB                       -- opcional, payload enviado
);

ALTER TABLE odonto_benef
  ADD CONSTRAINT pk_odonto_benef
  PRIMARY KEY (id_log);

CREATE SEQUENCE seq_odonto_benef START WITH 1 INCREMENT BY 1;
