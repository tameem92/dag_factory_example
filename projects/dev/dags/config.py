from airflow.models import Variable

# Fetch your basic airflow variables 
environment = Variable.get('environment', deserialize_json=True)
controller = Variable.get('controller', deserialize_json=True)

# Fetch DAG specific airflow variables - these can be setup 
# by your CI/CD.
dbt = Variable.get('dbt', deserialize_json=True)
