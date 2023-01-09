#Programa responsável por manter atualizado no DataMart as informações de posição de estoque que são extraídas de diversas tabelas que alimentam o ERP

#Definição da consulta padrão que será utilizada no processo
consulta = """
WITH CTECUSTO(NROEMPRESA, SEQPRODUTO, CUSTOBRUTOUNIT, DTAENTRADASAIDA) as
(SELECT 

nroempresa,
SEQPRODUTO,
CAST(avg(VLRCTOBRUTOUNIT) as decimal(10,4)),
max(DTAENTRADASAIDA)

FROM CONSINCO.MAXV_ABCMOVTOBASE_PROD
group by seqproduto, nroempresa),

TABELA_CUSTOBRT (SEQPRODUTO, SEQFILIAL, CUSTOBRUTOUNIT) AS
(select DISTINCT cte.seqproduto, cte.nroempresa, CAST(avg(a.VLRCTOBRUTOUNIT) AS DECIMAL(10,4)) AS CUSTOBRUTOUNIT from CTECUSTO cte
LEFT OUTER JOIN CONSINCO.MAXV_ABCMOVTOBASE_PROD a on a.seqproduto = cte.seqproduto

WHERE A.SEQPRODUTO = cte.SEQPRODUTO
AND A.NROEMPRESA = cte.NROEMPRESA
AND A.DTAENTRADASAIDA = cte.DTAENTRADASAIDA
AND A.NROEMPRESA IN (1, 2, 4, 5, 8, 9, 73, 77)
group by cte.seqproduto, cte.nroempresa
order by nroempresa asc)

SELECT
A.SEQPRODUTO,
A.NROEMPRESA,
A.MEDVDIAGERAL,
A.CLASSEABASTVLR,
A.ESTQLOJA,
A.ESTQDEPOSITO,
A.ESTQALMOXARIFADO,
A.ESTQTROCA,
A.ESTQOUTRO,
TO_CHAR(REPLACE(cbrt.CUSTOBRUTOUNIT, ',' , '.')) AS cto_bruto,
TO_CHAR(A.DTAULTVENDA, 'yyyy/mm/dd') as dtaultvenda


FROM CONSINCO.MRL_PRODUTOEMPRESA A
     LEFT JOIN TABELA_CUSTOBRT cbrt on A.SEQPRODUTO = cbrt.SEQPRODUTO
     
WHERE 
     A.SEQPRODUTO = cbrt.seqproduto
     AND A.NROEMPRESA = cbrt.SEQFILIAL
     
 order by a.seqproduto asc
"""

print('Importando bibliotecas...')
import cx_Oracle as cx
import mysql.connector as mysql

print('Criando conexão com a base de dados Oracle...')
#Cria conexão no database Oracle
ora_conn = cx.connect(user = 'usuario', password = 'senha', dsn = 'host/database')
ora_cursor = ora_conn.cursor()
print('Conexão estabelecida com a base de dados Oracle...')

print('Executando consulta na base de dados Oracle')
#Executa consulta no database Oracle
df = ora_cursor.execute(consulta).fetchall()

print('Consulta concluida!')
print('Conexão estabelecida com a base de dados Oracle...')
#Cria conexão no database MySQL
my_conn = mysql.connect(user = 'usuario', password = 'senha', host = 'host', database = 'prev_perdas')
my_cursor = my_conn.cursor()

print('Limpando tabela posicao_estq...')
#Limpa tabela posicao_estq
my_cursor.execute('TRUNCATE TABLE prev_perdas.posicao_estq')
my_conn.commit()

print('Iniciando inserção de dados na tabela posicao_estq.')
#Executa consulta de inserção de dados no database MySQL
for linha in df:
    c0 = linha[0] 
    c1 = linha[1]
    c2 = linha[2]
    c3 = linha[3]
    c4 = linha[4]
    c5 = linha[5]
    c6 = linha[6]
    c7 = linha[7]
    c8 = linha[8]
    c9 = linha[9]
    c10 = linha[10]
    my_cursor.execute(f"INSERT INTO prev_perdas.posicao_estq VALUES ('{c0}', '{c1}', '{c2}', '{c3}', '{c4}', '{c5}', '{c6}', '{c7}', '{c8}', '{c9}', '{c10}');")
my_conn.commit()
    
print('Atualização concluida com sucesso!')