from datetime import datetime, timedelta
import airflow  
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

def uploadFile():
    import boto3
    dt = datetime.now()
    year  = dt.year
    month = dt.month
    # day=dt.day
    # day=day-2
    hour = dt.hour
    min = dt.minute
    s3_path = "aws-glue-assets-079448565720-us-west-1"
    file_name = "/usr/local/airflow/dags/part-00000-a49d3d50-e67f-4a4e-8a9c-694f1e75ac88-c000.snappy.parquet"
    # object_name="UberDataHive/year={}/month={}/date={}/UberData-{}-{}.parquet".format(year,month,day,hour,min)

    s3_client = boto3.client('s3')
    for i in range(1,31):
         object_name="UberDataHive/year={}/month={}/date={}/UberData-{}-{}.parquet".format(year,month,i,hour,min)
         s3_client.upload_file(file_name, s3_path, object_name)
         time.sleep(1)

with DAG(
    dag_id='UploaduberDataCSVHiveStyle_2023', 
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