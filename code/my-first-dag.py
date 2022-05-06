# Importando as bibliotecas que vamos utilizar
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# definição de argumentos básicos
default_args = {
    "owner": "16ABD",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 6),
    "schedule_interval":'59 23 * * *', #..cada minuto
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
dag = DAG(
   'my-first-dag',
   schedule_interval=timedelta(days=1),
   default_args=default_args
   )
   
projectid ='noted-hangout-346322'   
topic_case ='topico_case'
topic_case_twiter ='topico_case_twiter'

# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
# O operador Bash, também pode ser utilizado para executar jobs Talend via Sh

m1 = {'data': b'Hello, World!',
      'attributes': {'type': 'greeting'}
     }

publish_task_sheets = PubSubPublishMessageOperator(
    task_id="ingestao_sheet",
    project_id=projectid,
    topic=topic_case,
    messages=[m1],
    dag=dag,
)  

publish_task_twitter = PubSubPublishMessageOperator(
    task_id="ingestao_twitter",
    project_id=projectid,
    topic=topic_case_twiter,
    messages=[m1],
    dag=dag,
) 
def procedures(task, procedure):
    sp_create_base_cluster = BigQueryOperator(
        task_id=task,
        sql=procedure,
        use_legacy_sql=False,
        dag=dag,
        depends_on_past=False)

    return sp_create_base_cluster
    
sp_cria_gera_relatorio = procedures("sp_cria_gera_relatorio","CALL procedure.sp_relatorios_gb()")    

publish_task_sheets >> sp_cria_gera_relatorio
publish_task_twitter
      
    
    
####https://github.com/brodriguesmclara/case_gb    