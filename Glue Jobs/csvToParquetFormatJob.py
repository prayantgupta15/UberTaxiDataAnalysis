import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
csv_path = "s3://aws-glue-assets-079448565720-us-west-1/UberData"
# s3://aws-glue-assets-079448565720-us-west-1/UberData/
parquet_path = "s3://aws-glue-assets-079448565720-us-west-1/UberData-parquet"

csv_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options = {"paths": [csv_path]},
    format = "csv",
    format_options={
        "withHeader": True    },
    )

parquet_dyf = glueContext.write_dynamic_frame_from_options(
    frame = csv_dyf,
    connection_type = "s3",
    connection_options = {"path":parquet_path},
    format = "parquet")

# parquet_dyf.prn

# reading from Parquet files

# parquet_dyf = glueContext.create_dynamic_frame_from_options(
#     connection_type = "s3",
#     connection_options = {"path":parquet_path},
#     format = "parquet")

# parquet_dyf.count()
job.commit()