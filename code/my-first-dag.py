# Importando as bibliotecas que vamos utilizar
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

# definição de argumentos básicos
default_args = {
    "owner": "16ABD",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "max_active_runs":1,
    "concurrency":4,
    "schedule_interval":'*/1 * * * *', #..cada minuto
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
dag = DAG(
   'my-first-dag',
   schedule_interval=timedelta(minutes=1),
   catchup=False,
   default_args=default_args
   )
   
projectid ='noted-hangout-346322'   
topic_case ='projects/noted-hangout-346322/topics/topico_case'


# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
# O operador Bash, também pode ser utilizado para executar jobs Talend via Sh

publish_task = PubSubPublishMessageOperator(
    task_id="publish_task",
    project_id=projectid,
    topic=topic_case,
    messages=[MESSAGE] * 10,
)  
publish_task    
    