CREATE TABLE odonto_associado_emp_chk (
  id_log        NUMBER PRIMARY KEY,
  nnumeusua     NUMBER,
  cpf           VARCHAR2(11),
  nome          VARCHAR2(100),
  status_api    VARCHAR2(20),    -- 'ENCONTRADO', 'NAO_ENCONTRADO', 'ERRO'
  http_status   NUMBER,
  msg_retorno   VARCHAR2(4000),
  json_retorno  CLOB,
  dt_execucao   DATE DEFAULT SYSDATE
);

CREATE SEQUENCE seq_odonto_ass_emp_chk;

select nnumeusua
  from odonto_associado_emp_chk
 where status_api = 'NAO_ENCONTRADO'
   and cpf = 03095847106