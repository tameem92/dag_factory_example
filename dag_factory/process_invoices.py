from datetime import datetime, timedelta
import os

from airflow.models import DAG, Variable
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


# Custom slack notifications
from utils.slack import task_fail_slack_alert
from utils.slack import pipeline_success_slack_alert

VERSION = 'v1.0.1'
YESTERDAY = datetime.utcnow() - timedelta(days=1)

class ProcessInvoices():

    def __init__(self, environment_config, config, organization_id=None):
        self.config = config
        self.environment_config = environment_config

        self.default_args = {
            'owner': self.environment_config.get('owner', 'flywheel'),
            'start_date': datetime.strptime(self.config['start_date'], '%Y-%m-%d'),
            'retries': 0,
            'on_failure_callback': task_fail_slack_alert,
        }

    def build(self):
        dag = DAG(
            dag_id=f'process_invoices_{VERSION}',
            default_args=self.default_args,
            schedule_interval=self.config['schedule'],
            catchup=False,
            template_searchpath=['/home/airflow/gcs/dags'],
        )

        secret_volume = Secret(
            'volume', # Path where we mount the secret as volume
            '/var/secrets/google', # Name of Kubernetes Secret
            'service-account', # Key in the form of service account file name
            'service-account.json'
        )
    
        process_invoices = KubernetesPodOperator(
            task_id='process_invoices',
            name='process_invoices',
            namespace='default',
            is_delete_operator_pod=True,
            image_pull_policy='Always',
            image_pull_secrets='gitlab-registry',
            image=self.config['image'],
            cmds=['/bin/bash', 'c', 'echo "hey"'],
            secrets=[secret_volume],
            dag=dag
        )

        return dag
