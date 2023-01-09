#Programa responsável por manter atualizado no DataMart as informações de cadastro de produtos que são extraídas de diversas tabelas que alimentam o ERP

# Definição da consulta padrão que será executada
consulta = """
SELECT DISTINCT

       A.SEQPRODUTO,
       B.CODACESSO,
       D.SEQCOMPRADOR,
       F.SEQFORNECEDOR,
       A.SEQFAMILIA,
       A.DESCCOMPLETA,
       C.FORMAABASTECIMENTO,    
       L.PESAVEL,
       CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 1, 'M') AS NIVEL1,
       CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 2, 'M') AS NIVEL2,
       CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 3, 'M') AS NIVEL3,
       CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 4, 'M') AS NIVEL4,
       CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 5, 'M') AS NIVEL5,
       L.FAMILIA,
       B.TIPCODIGO
       
  FROM CONSINCO.MAP_PRODUTO A     
  LEFT JOIN CONSINCO.MAP_PRODCODIGO B ON B.SEQPRODUTO = A.SEQPRODUTO 
  LEFT JOIN CONSINCO.MAP_FAMILIA L ON L.SEQFAMILIA = A.SEQFAMILIA
  LEFT JOIN CONSINCO.MAP_FAMDIVISAO C ON C.SEQFAMILIA = A.SEQFAMILIA
  LEFT JOIN CONSINCO.MAX_COMPRADOR D  ON D.SEQCOMPRADOR = C.SEQCOMPRADOR
  LEFT JOIN CONSINCO.MAP_FORMAABASTEC E ON E.FORMAABASTECIMENTO = C.FORMAABASTECIMENTO    
  LEFT JOIN CONSINCO.VIEW_BIG_FAMILIA_FORNECEDOR F ON F.SEQFAMILIA = A.SEQFAMILIA
  LEFT JOIN CONSINCO.AGE_FORNECEDOR G ON G.CODFORNECEDOR = F.SEQFORNECEDOR
  
  WHERE CONSINCO.fcategoriafamilianivel(A.SEQFAMILIA, 1, 1, 'M') <> 'ALMOXARIFADO' AND B.TIPCODIGO IN ('E', 'B')
"""

#Importação das bibliotecas que serão utilizadas no processo
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


print('Limpando tabela cad_produtos...')
#Limpa tabela cad_produto
my_cursor.execute('TRUNCATE TABLE prev_perdas.cad_produtos')
my_conn.commit()


print('Iniciando inserção de dados na tabela cad_produtos.')
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
    c11 = linha[11]
    c12 = linha[12]
    c13 = linha[13]
    c14 = linha[14]
    my_cursor.execute(f"INSERT INTO prev_perdas.cad_produtos (seqproduto, codacesso, seqcomprador, seqfornecedor, seqfamilia, desccompleta, formaabastecimento, pesavel, nivel1, nivel2, nivel3, nivel4, nivel5, descfamilia, tipcodigo) VALUES ('{c0}', '{c1}', '{c2}', '{c3}', '{c4}', '{c5}', '{c6}', '{c7}', '{c8}', '{c9}', '{c10}', '{c11}', '{c12}', '{c13}', '{c14}');")
    my_conn.commit()
    
print('Atualização concluida com sucesso!')

