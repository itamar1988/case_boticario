import gspread
from google.oauth2 import service_account
import pandas_gbq
import pandas as pd
import time

#Variaveis
project_id = 'noted-hangout-346322'
table_destination = 'raw.tb_vendas'
path_auth = 'auth.json'

# Autenticacao Google Sheets e BigQuery
cred = gspread.service_account(filename=path_auth)
sheets_id = ['1ebQKCjAJz1xlzCKALltdaKKdaXhkq-flNEE8Km5fJNU','1AEyrxYfWhZ3L07Bz_afx4dDrNe8CKCGgiAZ0mvgj1qo','1MtT98TsNbGJ0Mx4eUhW5g0t93mZuqn74CBOlXm-lio0']
creds_bq = service_account.Credentials.from_service_account_file(path_auth)

# Funcao para execucao da procedure criada previamente no BigQuery
#def call_procedure(project_id):
#  query = """call stored_procedure.sp_create_sales()"""
#  execute_proc = pandas_gbq.read_gbq(query,project_id = project_id,credentials=creds_bq)
#  return execute_proc
#  time.sleep(30)

"""
  Funcao main, faz conexao com as planilhas Google Sheets, faz o load dos dados das panilhas para 
   o BigQuery e ao finalizar o load ela executa a procedure responsavel pelo ETL e criação das 
   tabelas consolidadas.
"""
def vendas_sheets(event,context):
  for i in sheets_id:
    spreadsheet_id = cred.open_by_key(i)
    sheet_name = spreadsheet_id.worksheet('Planilha1')
    sheet_data = sheet_name.get_all_values()
    headers = sheet_data.pop(0)
    load_bq = pd.DataFrame(sheet_data,columns=headers)
    pandas_gbq.to_gbq(load_bq, table_destination, project_id = project_id, if_exists = 'append',credentials=creds_bq)
 #   df_execute_proc = call_procedure(project_id)