from datetime import datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth,month,date_format,year
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import corr
from pyspark.sql.functions import avg
import imp

def load_spark():
    my_spark = imp.load_source('my_spark', '/root/airflow/input_files/sparkjob.py')

default_args = {
    'start_date': datetime(2023, 6, 22),
    'email_on_failure': False,
    'email_on_success': True,
    'email': ['kavin.mani2000@gmail.com']
}
dag = DAG(
    'timber_dag',
    default_args=default_args,
    description='Airflow_project',
    schedule_interval='* * * * *',
    catchup=False,
    start_date=datetime(2023, 6, 22)
)
start_task = DummyOperator(task_id='start_task',dag=dag)


spark_task=PythonOperator(
    task_id='run_spark_task',
    python_callable=load_spark,
    dag=dag
)
end_task =DummyOperator(task_id='end_task',dag=dag)

success_notification = EmailOperator(
        task_id="success_notificatio",
        to=['kavin.mani2000@gmail.com'],
        subject="Alert Airflow Notification",
        html_content='<p>The task ran successfully.</p>',
        trigger_rule='all_success',
        cc=['kavin.mani2000@gmail.com'],
        dag=dag
    )

start_task >> spark_task >> end_task >>success_notification