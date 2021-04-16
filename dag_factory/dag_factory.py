from dag_factory.process_invoices import ProcessInvoices
from dag_factory.process_messages import ProcessMessages

class DAGFactory():  # pylint: disable=too-few-public-methods
    """The DAG Factory Class"""

    def __init__(self, environment_config=None, organization_id=None):
        self.environment_config = environment_config
        self.organization_id = organization_id

    def create(self, dag_type, config: list):
        supported_dags = {
            'ProcessInvoices': ProcessInvoices,
            'ProcessMessages': ProcessMessages
        }
        dag = supported_dags[dag_type](config=config, environment_config=self.environment_config, organization_id=self.organization_id)
        return dag


def reconfigure_subdag(parent_dag, sub_dag, sub_dag_name):
    """Reconfigures the DAG object (sub_dag) with attributes that must match
    the parent DAG object. e.g. the ID which must follow a specific convention,
    and the start_date.

    Args:
        parent_dag ([DAG]): [description]
        sub_dag ([DAG]): [description]
        sub_dag_name ([string]): [description]

    Returns:
        airflow.models.DAG: The modified DAG object
    """
    sub_dag.dag_id = parent_dag.dag_id + '.' + sub_dag_name
    sub_dag.start_date = parent_dag.start_date
    return sub_dag