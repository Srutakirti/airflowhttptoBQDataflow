from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator



bucket_name="srutakirtisparktests"
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)


def upload_to_gcs(file_path,**context):
    year, month, day, hour, *_ = context["execution_date"].timetuple()
    loc=f"{year}-{month:02}-{day:02}-{hour:02}"
    blob=bucket.blob(f"csvs/{loc}.csv")
    context['task_instance'].xcom_push(key="pickup_loc",
    value=f"gs://{bucket_name}" + f"/csvs/{loc}.csv")
    context['task_instance'].xcom_push(key="date",value=loc)
    with blob.open( mode='w') as b:
        with open(file_path,"r") as f:
            for line in f:
                b.write(line)


def write_to_temp(destination_file_name,response):
    with open( destination_file_name,mode='wb') as f:
        for chunk in response.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


def my_callable(execution_date,max_retries,task_instance):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    destination=f"/tmp/{year}-{month}-{day}-{hour}"
    destination_file_name=f"{destination}.csv.gz"
    task_instance.xcom_push(key="folder_name",value=destination)
    
    for _ in range(max_retries):
        r = requests.get(url,stream=True)
        if r:
            write_to_temp(destination_file_name=destination_file_name,response=r)
            return
    ##raise error
                


dag = DAG(
    dag_id='download_wikipedia_page_visits',
    description='download_page_visits',
    schedule_interval="@hourly",
    dagrun_timeout=timedelta(minutes=20),
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False)


download_gzip = PythonOperator(
    task_id='download_gzip',
    python_callable=my_callable,
    op_kwargs={"max_retries":3},
    dag=dag)

uncompress_gzip = BashOperator(
    task_id="uncompress_gzip",
    bash_command="gunzip -d -f {{task_instance.xcom_pull(task_ids='download_gzip', key='folder_name')}}.csv.gz"
)

upload_uncompressed_file = PythonOperator(
    task_id="upload_uncompressed_file",
    python_callable=upload_to_gcs,
    op_kwargs={"file_path":"{{task_instance.xcom_pull(task_ids='download_gzip', key='folder_name')}}.csv"}
)

delete_from_temp = BashOperator(
    task_id="delete_from_temp",
    bash_command="rm {{task_instance.xcom_pull(task_ids='download_gzip', key='folder_name')}}.csv"
)

start_template_job = DataflowTemplatedJobStartOperator(
    template="gs://srutakirti_dataflow/templates/third_template",
    task_id="start-template-job",
    parameters={"input_file":"{{task_instance.xcom_pull(task_ids='upload_uncompressed_file', key='pickup_loc')}}",
    "date":"{{task_instance.xcom_pull(task_ids='upload_uncompressed_file', key='date')}}"}
    
)


download_gzip >> uncompress_gzip >> upload_uncompressed_file >> delete_from_temp >> start_template_job

