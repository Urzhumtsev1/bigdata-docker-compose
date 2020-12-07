from  datetime import datetime as dt

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from livy_wrap import createSess, createSparkSession, closeSess, simpleTask

DAG_NAME = "airflow_livy_test"
SESS_VAR = DAG_NAME + "_sess"

dag = DAG(dag_id=DAG_NAME, schedule_interval=None, start_date=dt(2020, 12, 3))

createSess = PythonOperator(
    task_id='createSess', 
    python_callable=createSess, 
    op_args = [SESS_VAR], dag=dag
)
createSparkSess = PythonOperator(
    task_id='createSparkSess', 
    python_callable=createSparkSession, 
    op_args = [SESS_VAR], dag=dag
)
closeSess = PythonOperator(
    task_id='closeSess', 
    python_callable=closeSess, 
    op_args = [SESS_VAR], dag=dag
)

loadTab = PythonOperator(
    task_id='simpleTask', 
    python_callable=simpleTask, 
    op_args = [SESS_VAR, 1000], dag=dag
)

createSess >> createSparkSess >> loadTab >> closeSess
