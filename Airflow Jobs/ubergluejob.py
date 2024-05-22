from datetime import datetime, timedelta
import airflow  
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


defaultArgs = {
                "--enable-metrics":"true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://aws-glue-assets-079448565720-us-west-1/sparkHistoryLogs/",
                "--enable-job-insights": "true",
                "--job-language": "python"
            }
awsGlueServiceRole = 'AWSGlueServiceRoleDefault'
region_name = 'us-west-1'
s3_bucket='s3://aws-glue-assets-079448565720-us-west-1/scripts/'
with DAG(
    dag_id='TriggerUberGlueJob', 
    # schedule_interval='@once', 
    schedule_interval="0 12 * * *",
    catchup=False,
    start_date = datetime(2021,3,20)
) as dag:

    t1 = GlueJobOperator(
	task_id='Gluetask', 
	job_name="uberJobparquet", 
	s3_bucket = s3_bucket,
    concurrent_run_limit=1,
    retry_limit=0,
    region_name=region_name,
    wait_for_completion=True,
    verbose=True,
	script_location='s3://aws-glue-assets-079448565720-us-west-1/scripts/uberJobparquet.py',
    aws_conn_id='aws_default',
    iam_role_name=awsGlueServiceRole, 
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 3, "WorkerType": "G.1X","Connections":{
         "Connections": [
                    "myJDBCConn"
                ]
    },
     "DefaultArguments": defaultArgs
            
            },
	dag = dag
    )

    t2 = GlueJobOperator(
	task_id='analysis_task', 
	job_name="readUberTaxiDataFromRedshift", 
	s3_bucket = s3_bucket,
    concurrent_run_limit=1,
    retry_limit=0,
    region_name=region_name,
    wait_for_completion=True,
    verbose=True,
	script_location='s3://aws-glue-assets-079448565720-us-west-1/scripts/readUberTaxiDataFromRedshift.py',
    aws_conn_id='aws_default',
    iam_role_name=awsGlueServiceRole, 
    create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 3, "WorkerType": "G.1X","Connections":{
         "Connections": [
                    "myJDBCConn"
                ]
    },
     "DefaultArguments": defaultArgs
            
            },
	dag = dag
    )


    t1 >> t2


# Unknown parameter in JobUpdate: "connection", must be one of: Description, LogUri, Role, ExecutionProperty, Command, DefaultArguments, 
# NonOverridableArguments, Connections, MaxRetries, AllocatedCapacity, Timeout, MaxCapacity, WorkerType, NumberOfWorkers, SecurityConfiguration, 
# NotificationProperty, GlueVersion, CodeGenConfigurationNodes, ExecutionClass, SourceControlDetails   
 
# Learnt from error that for create_job_kwargs above paramters are valid
# [+] https://airflow.apache.org/docs/apache-airflow-providers-amazon/7.1.0/_modules/tests/system/providers/amazon/aws/example_glue.html
# [+] https://airflow.apache.org/docs/apache-airflow-providers-amazon/7.1.0/operators/glue.html