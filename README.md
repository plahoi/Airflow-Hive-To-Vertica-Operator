## Airflow Operator

### Hive To Vertica Operator

#### Usage:
```python
htv = HiveToVerticaOperator(
    hive_table='hive_schema.hive_table',
    vertica_table='tableau_schema.tableau_table',
    partition_column='day',
    partition_values='2019-04-28',
    dag=dag, task_id='anytask'
)
```

#### Dag example:

```python
import os
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators import HiveToVerticaOperator

ALERT_MAILS = Variable.get("gv_ic_admin_lst")
DAG_NAME = str(os.path.basename(__file__).split('.')[0])
OWNER = 'Polyakov Anton'
DEPENDS_ON_PAST = True
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False
RETRIES = int(Variable.get('gv_dag_retries'))
POOL = 'calc_pool'
MAIN_VAR_NAME = 'gv_' + DAG_NAME

WAIT_HRS = 1

start_dt = datetime.datetime(2019, 6, 25)
# setting default arguments of dag
default_args = {
    'owner': OWNER,
    'depends_on_past': DEPENDS_ON_PAST,
    'start_date': start_dt,
    'email': ALERT_MAILS,
    'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': EMAIL_ON_RETRY,
    'retries': RETRIES,
    'pool': POOL
}

# Creating DAG with parameters
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="0 9 * * *")
dag.doc_md = __doc__

dag_start = DummyOperator(
    task_id='dag_start',
    dag=dag
)

dag_end = DummyOperator(
    task_id='dag_end',
    dag=dag
)

htv = HiveToVerticaOperator(
    hive_table='hive_schema.hive_table',
    vertica_table='tableau_schema.tableau_table',
    partition_column='day',
    partition_values='2019-04-28',
    dag=dag, task_id='anytask'
)

dag_start >> htv
htv >> dag_end

```

