# Módulos do Airflow
from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@email.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=120),
    # Execute uma vez a cada 15 minutos 
    'schedule_interval': '*/15 * * * *'
}

with DAG(    
    dag_id='carga_antaq',
    default_args=default_args,
    schedule_interval=None,
    tags=['antaq'],
) as dag:    

    # Vamos Definir a nossa Primeira Tarefa 
    ingest = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/main_ingest.py", task_id="extracao_dados")

    #mover arquivos
    move_arq = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/mover_arquivo.py", task_id="move_arq")    
    
    # Vamos definir a nossa segunda tarefa    
    analyse = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/main_transformation.py", task_id="transformacao_dados")    
    
    carga_interna = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/carga_etl_interna.py", task_id="carga_etl_interna")    
    
    carga_cliente = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/carga_etl_cliente.py", task_id="carga_etl_cliente")    

    valida_processo    = BashOperator (bash_command="",task_id="valida_carga") 
    finaliza_processo  = BashOperator (bash_command="",task_id="finaliza_processo") 
    send_email         = EmailOperator(
                                        task_id='send_email',
                                        to='vamshikrishnapamucv@gmail.com',
                                        subject='Processo de carga',
                                        html_content='Erro na carga',
                                        dag=dag
                                        )
    
    ingest >> analyse >> [carga_interna, carga_cliente]
    ingest >> move_arq
    carga_interna >> valida_processo >> Label("Erro")    >> send_email
    carga_cliente >> valida_processo >> Label("Sucesso") >> finaliza_processo
    