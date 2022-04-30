
from airflow import DAG
import airflow
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

def insert_params(**context):
    param_dict={"input_file":"gs://srutakirtisparktests/csvs/2022-04-25-12.csv",
    "date":"2022-04-25-12"}
    context["task_instance"].xcom_push(key="param_dict",value=param_dict)

def print_params(params_dict,input_file,date,**context):
    print(params_dict)
    print(input_file)
    print(date)

dag = DAG(
dag_id='dataflowtestparams',
description='to_check _dataflow_params',
schedule_interval="@hourly",
dagrun_timeout=timedelta(minutes=20),
start_date=airflow.utils.dates.days_ago(1),
catchup=False)

insert_params_fortest = PythonOperator(
    task_id="insert_params_fortest",
    python_callable=insert_params,
    dag=dag
    )

print_params_fortest = PythonOperator(
    task_id="print_params_fortest",
    python_callable=print_params,
    op_kwargs={"params_dict":"{{task_instance.xcom_pull(task_ids='insert_params_fortest', key='param_dict')}}",
    "input_file":"{{task_instance.xcom_pull(task_ids='insert_params_fortest', key='param_dict')['input_file']}}",
    "date":"{{task_instance.xcom_pull(task_ids='insert_params_fortest', key='param_dict')['date']}}"
    }
)

start_template_job = DataflowTemplatedJobStartOperator(
    template="gs://srutakirti_dataflow/templates/third_template",
    task_id="start-template-job",
    parameters={"input_file":"{{task_instance.xcom_pull(task_ids='insert_params_fortest', key='param_dict')['input_file']}}",
    "date":"{{task_instance.xcom_pull(task_ids='insert_params_fortest', key='param_dict')['date']}}"}
    
)

insert_params_fortest >> print_params_fortest >> start_template_job