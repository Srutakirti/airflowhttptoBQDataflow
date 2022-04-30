from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator



def my_callable(**context):
    input_file="gs://srutakirtisparktests/sample_csv/sample.csv"
    output_file="gs://srutakirtisparktests/sample_csv/sample_test.csv"
    d={"in":input_file,"out":output_file}
    context['task_instance'].xcom_push(key="params",value=d)



dag = DAG(
    dag_id='run_beam_template',
    description='to_load_hourly_file_BQ',
    schedule_interval="@hourly",
    dagrun_timeout=timedelta(minutes=20),
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False)


create_xcoms = PythonOperator(
    task_id='create_xcoms',
    python_callable=my_callable,
    op_kwargs={"max_retries":3},
    dag=dag)

start_template_job = DataflowTemplatedJobStartOperator(
    template="gs://srutakirti_dataflow/templates/first_template",
    task_id="start-template-job",
    parameters={"input_file":"{{task_instance.xcom_pull(task_ids='create_xcoms', key='params')['in']}}",
    "output_file":"{{task_instance.xcom_pull(task_ids='create_xcoms', key='params')['out']}}"}
    
)

create_xcoms >> start_template_job





