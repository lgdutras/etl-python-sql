from datetime import date
from dateutil.relativedelta import relativedelta
from pandas import DataFrame, read_excel, read_sql_query, concat
import sys
sys.path.insert(0, r'C:/Users/GustavoDutra/projetos/conexao')
from conexao import conn, cursor # Aqui são importados os parâmetros de conexão aos bancos de dados necessários para o projeto

lojas = ['0012', '0013', '0014', '0015', '0018', '0019'] #Lista com o id das lojas em formato de string

class relatorio():
    
    # Atributos
    # Métodos
    def __init__(self, arquivo):

        global lojas
        self.arquivo = arquivo
        self.relacao = DataFrame(read_excel(f'C:/Users/GustavoDutra/Desktop/Teste/{arquivo}', usecols=['Codigo', 'Quant', 'Aplicação']))
        self.fornecedor = str(read_excel(f'C:/Users/GustavoDutra/Desktop/Teste/{arquivo}', usecols=['Fornecedor']))
        self.devolucao = DataFrame()
        self.espelho_devolucao = DataFrame()

# No caso das notas de entrada serem do depósito central, é utilizado o método MontaDev_Central.
# Ele tem menor desempenho por ler menos notas de entrada

    def MontaDev_Central(self):
        d = date.today()
        MinDate = d.replace(day=1) - relativedelta(months=6)
        for i in self.relacao.iterrows():
            seqitem = {i[1][0]}
            query = f'''SELECT
                            a.dtaentrada,
                            a.seqproduto,
                            a.numerodf,
                            a.nroempresa,
                            a.quantidade,
                            COALESCE(a.qtddevolvida,0) as qtddevolvida,
                            (a.quantidade - COALESCE(a.qtddevolvida, 0)) as saldo_devolucao
                            
                                            FROM ~DBNAME.DB_TABLE~ A
                                            
                                            WHERE a.codgeraloper = 1
                                            AND a.seqproduto = {seqitem}
                                            AND a.nroempresa = 77
                                            
                            ORDER BY a.dtaentrada asc'''
            tmp_saldo_notas = read_sql_query(query, conn)
            if tmp_saldo_notas.shape[0] == 0:
                print('Não há entradas nesta filial para este produto')
            else:
                while qtd > 0:
                    for index, linha in tmp_saldo_notas[tmp_saldo_notas['DTAENTRADA'] > f'{MinDate}'].iterrows():
                        if (linha['SALDO_DEVOLUCAO'] >= qtd):
                            tmp_devolucao = DataFrame(data = (str(linha['SEQPRODUTO']), str(linha['NUMERODF']), str(qtd)), columns = ['seqproduto', 'numerodocto', 'qtd_devolucao'])
                            self.devolucao = concat([tmp_devolucao, self.devolucao])
                            qtd = qtd - linha['SALDO_DEVOLUCAO']
                            break
                        elif (linha['SALDO_DEVOLUCAO']) < qtd:
                            tmp_devolucao = DataFrame(data = (str(linha['SEQPRODUTO']), str(linha['NUMERODF']) , str(linha['SALDO_DEVOLUCAO'])), columns = ['seqproduto', 'numerodocto', 'qtd_devolucao'])
                            qtd = qtd - linha['SALDO_DEVOLUCAO']

# getEstqTroca Em desenvolvimento -- Futuramente será parte do método MontaDev_Filiais ...
    def getEstqTroca(self):
        for i in self.relacao.iterrows():
            troca_total = (read_sql_query(f"SELECT SUM(ESTQTROCA) as ESTQ FROM ~DBNAME.DB_TABLE~ WHERE SEQPRODUTO = '{i[1][0]}' AND NROEMPRESA IN ({', '.join(lojas)})", conn))['ESTQ'][0]
            qtd_devolucao = int(i[1][1])
            for loja in lojas:
                estq_loja = {}
                q_troca_loja = f"SELECT ESTQTROCA AS ESTQ FROM ~DBNAME.DB_TABLE~ A WHERE A.SEQPRODUTO = '{i[1][0]}' AND A.NROEMPRESA = {loja}"
                estq = read_sql_query(q_troca_loja, conn)
                estq_loja


# Declarando e chamando para testes...
craft = relatorio('011122_FORNECEDOR') # relatorio emitido pela equipe operacional DATAEMISSAO_NOMEFORNECEDOR
craft.getEstqTroca()