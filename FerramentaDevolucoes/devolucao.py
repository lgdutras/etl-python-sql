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
        self.EspelhoDevolucao = DataFrame()
        self.MontaDevCentral()

# TO DO - CONSULTAR DADOS TRIBUTÁRIOS DA COMPRA PARA GERAR ESPELHO DE DEVOLUÇÃO
    def MontaEspelhoDev(self):
        for produto, nota in self.devolucao:
            queryInfoTributos = '''... Consulta SQL Aqui ...'''
            dfInfoTributos = read_sql_query(queryInfoTributos, conn)
            self.EspelhoDevolucao = 1 # Merge devolucao + dfInfoTributos

# No caso das notas de entrada serem do depósito central, é utilizado o método MontaDev_Central.
# Ele tem maior desempenho por ler menos notas de entrada

    def MontaDevCentral(self):
        hoje = date.today()
        MinDate = hoje.replace(day=1) - relativedelta(months=6)
        for index, produto in self.relacao.iterrows():
            seqitem = produto['Codigo']
            qtdDevolucao = int(produto['Quant'])
            queryListaNotas = f'''SELECT
                            a.dtaentrada,
                            a.seqproduto,
                            a.numerodf,
                            a.nroempresa,
                            a.quantidade,
                            COALESCE(a.qtddevolvida,0) as qtddevolvida,
                            (a.quantidade - COALESCE(a.qtddevolvida, 0)) as saldo_devolucao
                            
                                            FROM ~DBNAME.DBTABLE~ A
                                            
                                            WHERE a.codgeraloper = 1
                                            AND a.seqproduto = {seqitem}
                                            AND a.nroempresa = 77
                                            AND a.dtaentrada > TO_DATE(SYSDATE) - 180
                                            AND (a.quantidade - COALESCE(a.qtddevolvida, 0)) > 0
                                            
                            ORDER BY a.dtaentrada asc'''
            dfTempSaldoNotas = DataFrame(read_sql_query(queryListaNotas, conn))
            if dfTempSaldoNotas.shape[0] == 0:
                print('Não há entradas nesta filial para este produto')
            else:
                while qtdDevolucao > 0:
                    for index, nota in dfTempSaldoNotas.iterrows():
                        if (nota['SALDO_DEVOLUCAO'] >= qtdDevolucao and qtdDevolucao > 0):
                            dfTempDevolucao = DataFrame(data = [(str(nota['SEQPRODUTO']), str(nota['NROEMPRESA']), str(nota['NUMERODF']) , str(qtdDevolucao))], columns = ['seqproduto', 'nroempresa', 'numerodocto', 'qtd_devolucao'])
                            self.devolucao = concat([dfTempDevolucao, self.devolucao])
                            qtdDevolucao = qtdDevolucao - nota['SALDO_DEVOLUCAO']
                            break
                        else:
                            while qtdDevolucao > 0:
                                for index, nota in dfTempSaldoNotas.iterrows():
                                    if nota['SALDO_DEVOLUCAO'] <= qtdDevolucao:
                                        dfTempDevolucao = DataFrame(data = [(str(nota['SEQPRODUTO']), str(nota['NROEMPRESA']), str(nota['NUMERODF']) , str(nota['SALDO_DEVOLUCAO']))], columns = ['seqproduto', 'nroempresa', 'numerodocto', 'qtd_devolucao'])
                                        self.devolucao = concat([dfTempDevolucao, self.devolucao])
                                        qtdDevolucao = qtdDevolucao - nota['SALDO_DEVOLUCAO']
                                    else:
                                        dfTempDevolucao = DataFrame(data = [(str(nota['SEQPRODUTO']), str(nota['NROEMPRESA']), str(nota['NUMERODF']) , str(qtdDevolucao))], columns = ['seqproduto', 'nroempresa', 'numerodocto', 'qtd_devolucao'])
                                        self.devolucao = concat([dfTempDevolucao, self.devolucao])
                                        qtdDevolucao = qtdDevolucao - nota['SALDO_DEVOLUCAO']
                                        break


# TO DO - Futuramente a função getEstqTroca será parte do método MontaDevFiliais ...
    def getEstqTroca(self):
        for produto in self.relacao.iterrows():
            TrocaTotal = (read_sql_query(f"SELECT SUM(ESTQTROCA) as ESTQ FROM ~DBNAME.DBTABLE~ WHERE SEQPRODUTO = '{i[1][0]}' AND NROEMPRESA IN ({', '.join(lojas)})", conn))['ESTQ'][0]
            qtdDevolucao = int(produto[1][1])
            for loja in lojas:
                EstqLoja = {}
                queryTrocaLoja = f"SELECT ESTQTROCA AS ESTQ FROM ~DBNAME.DBTABLE~ A WHERE A.SEQPRODUTO = '{i[1][0]}' AND A.NROEMPRESA = {loja}"
                estq = read_sql_query(queryTrocaLoja, conn)
                EstqLoja


# Declarando e chamando para testes...
craft = relatorio('011122_CRAFT')
print(craft.devolucao)