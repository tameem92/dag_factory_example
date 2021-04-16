from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.subdag_operator import SubDagOperator

import config
from dag_factory.dag_factory import DAGFactory, reconfigure_subdag
from utils.slack import task_fail_slack_alert
from utils.slack import pipeline_success_slack_alert


VERSION='v1.0.0'

# Initialize DAG Factory
factory = DAGFactory(config.environment)

# Build DAGs using the factory
process_invoices_dag = factory.create('ProcessInvoicesDAG', config.process_invoices).build()
process_messages_dag = factory.create('ProcessMessagesDAG', config.process_messages).build()

# Main DAG default args
default_args = {
    'owner': config.environment['owner'],
    'start_date': datetime.strptime(config.controller['start_date'], '%Y-%m-%d'),
    'retries': 0, # applies to subdags only
    'retry_delay': timedelta(minutes=config.environment['retry_delay_minutes']),
    'on_failure_callback': task_fail_slack_alert,
    'executor': CeleryExecutor()
}

with DAG(dag_id=f'main_{VERSION}',
         default_args=default_args,
         schedule_interval=config.controller['schedule'],
         catchup=False,
         template_searchpath=['/home/airflow/gcs/dags'],
    ) as dag:
  
    # Load GCS to BigQuery
    subdag_1 = SubDagOperator(
        task_id='task_1',
        subdag=reconfigure_subdag(dag, task_1_dag, 'task_1_name')
    )

    subdag_2 = SubDagOperator(
        task_id='task_2',
        subdag=reconfigure_subdag(dag, task_2_dag, 'task_2_name')
    )