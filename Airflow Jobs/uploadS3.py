from datetime import datetime, timedelta
import airflow  
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def uploadFile():
    import boto3
    dt = datetime.now()
    year  = dt.year
    month = dt.month
    day=dt.day
    hour = dt.hour
    min = dt.minute
    s3_path = "aws-glue-assets-079448565720-us-west-1"
    file_name = "/usr/local/airflow/dags/uber_data.csv"
    object_name="UberData/UberData-{}-{}-{}-{}-{}.csv".format(year,month,day,hour,min)

    s3_client = boto3.client('s3')

    s3_client.upload_file(file_name, s3_path, object_name)

with DAG(
    dag_id='UploaduberDataCSV', 
    # schedule_interval='@once', 
    schedule_interval="*/2 * * * *",
    catchup=False,
    start_date = datetime(2021,3,20)
) as dag:

     task1 = PythonOperator(
        task_id="uploadCSV",
        python_callable= uploadFile,
        dag=dag
    )