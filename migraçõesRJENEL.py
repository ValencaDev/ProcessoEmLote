import mysql.connector
import pandas as pd
from datetime import date, datetime
import tkinter as tk
from tkinter import filedialog
import os
from dotenv import load_dotenv 

load_dotenv()
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE'),
    'port': 3306  # se estiver usando uma porta personalizada, mude aqui
}

ARQUIVO_PLANILHA = ''
SHEET_NAME = 0

colunas_thproc = [
    'cnj', 'tipoPartePoloAtivo', 'partePoloAtivo', 'tipoPartePoloPassivo',
    'partePoloPassivo', 'cliente', 'tipoDeRito', 'dataDistribuicao',
    'numeroUnidade', 'unidade', 'especialidade', 'comarca', 'estado',
    'orgao', 'natureza', 'materia', 'dataInstancia', 'tipoInstancia',
    'valorCausa', 'tipoAcao', 'tipoObjeto', 'dataFase', 'fase', 'dataStatus',
    'status', 'carteira', 'prioridadeDe', 'dataEvento', 'tipoEvento',
    'descricaoEvento', 'solicitanteEvento', 'responsavelEvento',
    'corresponsavel', 'teste', 'pasta', 'numeroProcessoAnterior',
    'numeroProcessoCNJ', 'sistemaExterno', 'processoEletronico',
    'processoEstrategico', 'grupoProcesso', 'complementoEvento',
    'observacaoEvento', 'grupoTrabalho', 'dataNotificacao',
    'dataNotificacaoAdicional', 'probabilidadePerda',
    'dataValorProvisionado', 'valorProvisionado', 'dataAndamento',
    'tipoAndamento', 'descricaoAndamento', 'complementoAndamento',
    'solicitanteAndamento', 'responsavelAndamento', 'corresponsavelAndamento',
    'descricaoObjeto', 'escritorioCredenciado', 'data_hora_verificacao',
    'usuario_verificado_id', 'verificado', 'nomegrupo_id', 'cliente_id',
    'exportado', 'dataContratacao', 'observacaoDoProcesso',
    'parecerDoProcesso', 'data_hora_submit', 'data_hora_export',
    'usuario_submit_id', 'usuario_export_id', 'valorFinalCausa',
    'tipoPoloCliente', 'data_resultado', 'tipo_resultado',
    'descricao_resultado', 'Adv_parte_contraria', 'codnatureza',
    'codparte_polo_ativo', 'codpolo_cliente', 'codsistema_externo',
    'codstatus', 'codfase', 'codespecialidade', 'codorgao', 'codmateria',
    'codtipo_rito', 'codcomarca', 'codparte_polo_passivo', 'codunidade',
    'codtipo_instancia', 'codcorresponsavelAndamento', 'codlote'
]

def inserir_dados_thproc(data_records):
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        column_names = ", ".join(colunas_thproc)
        placeholders = ", ".join(["%s"] * len(colunas_thproc))

        sql_insert = f"INSERT INTO thproc ({column_names}) VALUES ({placeholders})"

        registros_para_execucao = []
        for record in data_records:
            valores = []
            for coluna in colunas_thproc:
                valor = record.get(coluna, None)
                # Converte NaN para None
                if pd.isna(valor):
                    valores.append(None)
                elif isinstance(valor, pd.Timestamp):
                    if coluna.startswith('data_hora'):
                        valores.append(valor.to_pydatetime())
                    elif coluna.startswith('data'):
                        valores.append(valor.date())
                    else:
                        valores.append(valor)
                else:
                    valores.append(valor)
            registros_para_execucao.append(tuple(valores))

        print(f"Preparando para inserir {len(registros_para_execucao)} registros na tabela thproc...")
        cursor.executemany(sql_insert, registros_para_execucao)
        conn.commit()

        print(f"{cursor.rowcount} registros inseridos com sucesso na tabela thproc.")

    except mysql.connector.Error as err:
        print(f"Erro ao inserir dados no MySQL: {err}")
        if conn and conn.is_connected():
            conn.rollback()
            print("Transação revertida (rollback).")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Conexão MySQL fechada.")

if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()

    ARQUIVO_PLANILHA = filedialog.askopenfilename(
        title="Selecione a planilha Excel para importação",
        filetypes=[("Arquivos Excel", "*.xlsx *.xls")]
    )

    if not ARQUIVO_PLANILHA:
        print("Nenhuma planilha selecionada. O script será encerrado.")
    else:
        try:
            print(f"Tentando ler a planilha Excel em: {ARQUIVO_PLANILHA} (Aba: {SHEET_NAME})")
            df = pd.read_excel(ARQUIVO_PLANILHA, sheet_name=SHEET_NAME)
            df = df.rename(columns={
                'Pasta': 'pasta',
                'Nº do Processo Anterior': 'numeroProcessoAnterior',
                'Nº do Processo CNJ': 'cnj',
                'Tipo de Parte Pólo Ativo': 'tipoPartePoloAtivo',
                'Parte Pólo Ativo': 'partePoloAtivo',
                'Tipo de Parte Pólo Passivo': 'tipoPartePoloPassivo',
                'Parte Pólo Passivo': 'partePoloPassivo',
                'Cliente': 'cliente',
                'Tipo de Rito': 'tipoDeRito',
                'Data de Distribuição': 'dataDistribuicao',
                'Número Unidade': 'numeroUnidade',
                'Unidade': 'unidade',
                'Especialidade': 'especialidade',
                'Comarca': 'comarca',
                'Estado': 'estado',
                'Órgão': 'orgao',
                'Natureza': 'natureza',
                'Matéria': 'materia',
                'Data da Instância': 'dataInstancia',
                'Tipo de Instância': 'tipoInstancia',
                'Sistema Externo': 'sistemaExterno',
                'Processo Eletrônico': 'processoEletronico',
                'Processo Estratégico': 'processoEstrategico',
                'Valor da Causa': 'valorCausa',
                'Valor Final da Causa': 'valorFinalCausa',
                'Tipo de Ação': 'tipoAcao',
                'Tipo de Objeto': 'tipoObjeto',
                'Data da Fase': 'dataFase',
                'Fase': 'fase',
                'Data do Status': 'dataStatus',
                'Status': 'status',
                'Grupo de Processo': 'grupoProcesso',
                'Prioridade De': 'prioridadeDe',
                'Data do Resultado': 'data_resultado',
                'Tipo de Resultado': 'tipo_resultado',
                'Descrição do Resultado': 'descricao_resultado',
                'Data Evento': 'dataEvento',
                'Tipo Evento': 'tipoEvento',
                'Descrição Evento': 'descricaoEvento',
                'Complemento Evento': 'complementoEvento',
                'Observação Evento': 'observacaoEvento',
                'Solicitante Evento': 'solicitanteEvento',
                'Responsável Evento': 'responsavelEvento',
                'Grupo de Trabalho': 'grupoTrabalho',
                'Corresponsável': 'corresponsavel',
                'Data de Notificação': 'dataNotificacao',
                'Data de Notificação Adicional': 'dataNotificacaoAdicional',
                'Probabilidade de Perda': 'probabilidadePerda',
                'Data do valor provisionado': 'dataValorProvisionado',
                'Valor Provisionado': 'valorProvisionado',
                'Data do Andamento': 'dataAndamento',
                'Tipo do Andamento': 'tipoAndamento',
                'Descrição do Andamento': 'descricaoAndamento',
                'Complemento do Andamento': 'complementoAndamento',
                'Solicitante do Andamento': 'solicitanteAndamento',
                'Responsável do Andamento': 'responsavelAndamento',
                'Corresponsável do Andamento': 'corresponsavelAndamento',
                'Descrição do objeto': 'descricaoObjeto',
                'Escritório Credenciado': 'escritorioCredenciado',
                'Data da Contratação': 'dataContratacao',
                'Observação do Processo': 'observacaoDoProcesso',
                'Parecer do Processo': 'parecerDoProcesso',
            })
            df['cnj'] = df['cnj']

            today_date = datetime.now().strftime('%d/%m/%Y')
            # df['estado'] = 'RJ'

            df['tipoPoloCliente'] ='Passivo'
            df['codpolo_cliente'] = '2'

            df['dataNotificacao'] = 'Agora'
            df['dataNotificacaoAdicional'] = 'No dia do Evento'


            ##Enel
            df['nomegrupo_id'] = '101'
            df['solicitanteAndamento'] = 438
            df['responsavelAndamento'] = 353
            df['corresponsavelAndamento'] = 438
            df['prioridadeDe'] = 438
            df['orgao'] = 'TJ - RJ'
            df['tipoEvento'] = 1035
            df['corresponsavel'] = 32
            df['codlote'] = 'Enel ' + today_date
            df['carteira'] = '101'


            ##Stone
            ##df['nomegrupo_id'] = '49'
            ##df['prioridadeDe'] = 102
            ##df['solicitanteAndamento'] = 102
            ##df['responsavelAndamento'] = 102
            ##df['corresponsavelAndamento'] = 102
            ##df['orgao'] = 'STJ'
            ##df['corresponsavel'] = 30
            ##df['codlote'] = 'Stone' + today_date
            ##df['carteira'] = '49'
            ##df['tipoEvento'] = 979
            ##df['solicitanteEvento'] = 102
            ##df['descricaoObjeto'] = df['tipoObjeto']

            ##Cagece
            ## df['nomegrupo_id'] = '6'
            ## df['prioridadeDe'] = 135
            ## df['solicitanteAndamento'] = 135
            ## df['responsavelAndamento'] = 135
            ## df['corresponsavelAndamento'] = 135
            ## df['orgao'] = 'TJ - CE'
            ## df['corresponsavel'] = 30
            ## df['codlote'] = 'Cagece'  + today_date
            ## df['carteira'] = '6'
            ## df['tipoEvento'] = 979
            ## df['solicitanteEvento'] = 135
            ## df['tipoAndamento'] = 'Não Informado'
            ## df['descricaoObjeto'] = 'Não Informado'

            ### NOTREDAME-CIVEL
            ##df['nomegrupo_id']            = '21'
            ##df['prioridadeDe']            = 186
            ##df['solicitanteAndamento']    = 186
            ##df['responsavelAndamento']    = 186
            ##df['corresponsavelAndamento'] = 186
            ##df['orgao']                   = 'TJ - SP'
            ##df['corresponsavel']          = 6
            ##df['codlote']                 = 'NOTREDAME-CIVEL' + today_date
            ##df['carteira']                = '21'
            ##df['tipoEvento']              = 380
            ##df['solicitanteEvento']       = 186
            ##df['tipoAndamento']           = 'Não Informado'
            ##df['descricaoObjeto']         = 'Não Informado'

            ###NOTREDAME- Estrategico
            #df['nomegrupo_id'] = '52'
            #df['prioridadeDe'] = 481
            #df['solicitanteAndamento'] = 481
            #df['responsavelAndamento'] = 481
            #df['corresponsavelAndamento'] = 481
            #df['orgao'] = 'TJ - SP'
            #df['corresponsavel'] = 33
            #df['codlote'] = 'NOTREDAME-ESTRATEGICO' + today_date
            #df['carteira'] = '52'
            #df['tipoEvento'] = 1049
            #df['solicitanteEvento'] = 481
            #df['tipoAndamento'] = 'Não Informado'
            #df['descricaoObjeto'] = 'Não Informado'

            ### NOTREDAME- TRAB
            #df['nomegrupo_id'] = '48'
            #df['prioridadeDe'] = 477
            #df['solicitanteAndamento'] = 477
            #df['responsavelAndamento'] = 477
            #df['corresponsavelAndamento'] = 477
            #df['orgao'] = 'TRT - 22º REGIAO'
            #df['corresponsavel'] = 31
            #df['codlote'] = 'NOTREDAME-TRABALISTA' + today_date
            #df['carteira'] = '48'
            #df['tipoEvento'] = 1005
            #df['solicitanteEvento'] = 477
            #df['tipoAndamento'] = 'Não Informado'
            #df['descricaoObjeto'] = 'Não Informado'

            ### NOTREDAME- PORTO SEGURO
            ##df['nomegrupo_id'] = '27'
            ##df['prioridadeDe'] = 124
            ##df['solicitanteAndamento'] = 124
            ##df['responsavelAndamento'] = 124
            ##df['corresponsavelAndamento'] = 124
            ##df['orgao'] = 'TJ - SP'
            ##df['corresponsavel'] = 7
            ##df['codlote'] = 'PORTOSEGURO' + today_date
            ##df['carteira'] = '27'
            ##df['tipoEvento'] = 431
            ##df['solicitanteEvento'] = 124
            ##df['tipoAndamento'] = 'Não Informado'
            ##df['descricaoObjeto'] = 'Não Informado'

            df['escritorioCredenciado'] = 'VALENÇA & ASSOCIADOS'
            # usuário e exportado e verificado
            df['usuario_submit_id'] = 420
            df['verificado'] = 0
            df['exportado'] = 0


            # Preencher campo obrigatório 'tipoObjeto' com valor padrão caso esteja vazio
            if 'tipoObjeto' in df.columns:
                df['tipoObjeto'] = df['tipoObjeto'].fillna('Não Informado')  # ou outro valor padrão

            if 'fase' in df.columns:
                df['fase'] = df['fase'].fillna('Conhecimento')


            if 'descricaoEvento' in df.columns:
                df['descricaoEvento'] = df['descricaoEvento'].fillna('Não Informado')

            if 'solicitanteEvento' in df.columns:
                df['solicitanteEvento'] = df['solicitanteEvento'].fillna('438')

            if 'responsavelEvento' in df.columns:
                df['responsavelEvento'] = df['responsavelEvento'].fillna('438')

            if 'valorFinalCausa' in df.columns:
                df['valorFinalCausa'] = pd.to_numeric(df['valorFinalCausa'], errors='coerce').fillna(0.0)

            if 'valorCausa' in df.columns:
                df['valorCausa'] = pd.to_numeric(df['valorCausa'], errors='coerce').fillna(0.0)

            if 'valorProvisionado' in df.columns:
                df['valorProvisionado'] = pd.to_numeric(df['valorProvisionado'], errors='coerce').fillna(0.0)


            # Preencher dataEvento com a data de hoje caso esteja vazia
            if 'dataEvento' in df.columns:
                df['dataEvento'] = pd.to_datetime(df['dataEvento'], errors='coerce', dayfirst=True)
                df['dataEvento'] = df['dataEvento'].fillna(pd.to_datetime(date.today()))
                df['dataEvento'] = df['dataEvento'].dt.date

            if 'dataFase' in df.columns:
                df['dataFase'] = pd.to_datetime(df['dataFase'], errors='coerce', dayfirst=True)
                df['dataFase'] = df['dataFase'].fillna(pd.to_datetime(date.today()))
                df['dataFase'] = df['dataFase'].dt.date

            if 'dataContratacao' in df.columns:
                df['dataContratacao'] = pd.to_datetime(df['dataContratacao'], errors='coerce', dayfirst=True)
                df['dataContratacao'] = df['dataContratacao'].fillna(pd.to_datetime(date.today()))
                df['dataContratacao'] = df['dataContratacao'].dt.date

            if 'data_hora_submit' in df.columns:
                df['data_hora_submit'] = pd.to_datetime(df['data_hora_submit'], errors='coerce', dayfirst=True)
                df['data_hora_submit'] = df['data_hora_submit'].fillna(pd.to_datetime(date.today()))
                df['data_hora_submit'] = df['data_hora_submit'].dt.date

            if 'dataInstancia' in df.columns:
                df['dataInstancia'] = pd.to_datetime(df['dataInstancia'], errors='coerce', dayfirst=True)
                df['dataInstancia'] = df['dataInstancia'].fillna(pd.to_datetime(date.today()))
                df['dataInstancia'] = df['dataInstancia'].dt.date

            if 'dataDistribuicao' in df.columns:
                df['dataDistribuicao'] = pd.to_datetime(df['dataDistribuicao'], errors='coerce', dayfirst=True)
                df['dataDistribuicao'] = df['dataDistribuicao'].fillna(pd.to_datetime(date.today()))
                df['dataDistribuicao'] = df['dataDistribuicao'].dt.date

            # Converter datas para formato compatível com o MySQL
            colunas_data = [
                'dataDistribuicao', 'dataInstancia', 'dataFase', 'dataStatus',
                'dataEvento',
                'dataValorProvisionado', 'dataAndamento', 'dataContratacao',
                'data_resultado'
            ]


            # Truncar campos de texto longos que têm limite de 100 caracteres
            campos_texto_100 = [
                'partePoloAtivo', 'partePoloPassivo', 'cliente', 'tipoDeRito',
                'unidade', 'especialidade', 'comarca', 'estado', 'orgao', 'natureza',
                'materia', 'tipoInstancia', 'tipoAcao', 'tipoObjeto', 'fase', 'status',
                'carteira', 'prioridadeDe', 'tipoEvento', 'solicitanteEvento',
                'responsavelEvento', 'corresponsavel', 'sistemaExterno',
                'tipoAndamento', 'solicitanteAndamento', 'responsavelAndamento',
                'corresponsavelAndamento', 'descricaoObjeto', 'escritorioCredenciado',
                'tipoPoloCliente', 'tipo_resultado', 'Adv_parte_contraria'
            ]

            for campo in campos_texto_100:
                if campo in df.columns:
                    df[campo] = df[campo].astype(str).str.slice(0, 100)
                    
            for col in colunas_data:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date





            dados_para_inserir = df.to_dict(orient='records')

            inserir_dados_thproc(dados_para_inserir)

        except FileNotFoundError:
            print(f"ERRO: O arquivo da planilha '{ARQUIVO_PLANILHA}' não foi encontrado.")
            print("Por favor, verifique o caminho e o nome do arquivo.")
        except pd.errors.EmptyDataError:
            print(f"ERRO: O arquivo '{ARQUIVO_PLANILHA}' está vazio.")
        except KeyError as ke:
            print(f"ERRO DE COLUNA: Uma coluna esperada pelo script não foi encontrada na planilha ou tem nome diferente: {ke}")
            print("Verifique se os nomes das colunas na sua planilha Excel são idênticos aos nomes no banco de dados, ou se você precisa usar a seção de 'df.rename(columns={{...}})'.")
        except Exception as e:
            print(f"Ocorreu um erro geral ao processar a planilha ou inserir dados: {e}")