##apache-airflow-providers-google 6.8.0 requires google-cloud-storage<2.0.0,>=1.30, 
#but you have google-cloud-storage 2.3.0 which is incompatible.

from google.cloud import storage
import requests

year="2020"
month="03"
day="22"
hour="21"
output_path="test.gz"

# bucket_name="us-central1-airflowlearn-7a98eef0-bucket"
# destination_blob_name="dags/airflowbeamtemplatedag.py"
# file_to_upload="airflowbeamtemplatedag.py"

# bucket_name="srutakirtisparktests"
# destination_blob_name="sample_csv/sample.csv"
# file_to_upload="tofile.csv"


bucket_name="us-central1-aiflowwithdataf-68dff654-bucket"
destination_blob_name="dags/airflowbeamtemplatedag.py"
file_to_upload="dataflowtestparams.py"



url=f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"


storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

c=0
r = requests.get(url,stream=True)


# with blob.open( mode='wb') as f:
#     for chunk in r.iter_content(chunk_size=1024): 
#         if chunk: # filter out keep-alive new chunks
#             f.write(chunk)
#             print(c)
#             c+=1

with blob.open( mode='w') as b:
    with open(file_to_upload,"r") as f:
        for line in f:
            b.write(line)
