import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# doing test runs with different configs to check the joining strategies
# spark.conf.set('spark.sql.autoBroadcastJoinThreshold','-1')
spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled','false')
# print(spark.conf.get('spark.sql.shuffle.partitions'))

# spark.conf.set('spark.sql.autoBroadcastJoinThreshold','52428800')
# spark.conf.set('spark.sql.adaptive.enabled','true')

# facttable
# my_conn_options = {"url": "redshift-cluster-1.cnuu7fxh4kr4.us-west-1.redshift.amazonaws.com:5439/dev", 
# "user": "awsuser",
# "password": "Awsuser12345",
# "dbtable": "facttable_partitioned",
# "redshiftTmpDir": "s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"

# # ,
# #         "hashfield":"date",
# #     "hashpartitions":"10"

# } 

# dyf = glueContext.create_dynamic_frame_from_options(connection_type= "redshift",connection_options =  my_conn_options)
# print(dyf.count())

facttable_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties":"true",
        "connectionName":"myJDBCConn",
        "dbtable":"facttable_partitioned",
            # "ratecodesdim_partitioned",
            
            # "facttable_parq",
        "redshiftTmpDir":"s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
    #     ,
    #     "hashfield":"uploaddate",
    # "hashpartitions":"10"
    }
    )
# print(facttable_dyf.count())
# facttable_df.show()
# facttable_df.printSchema()
facttable_df = facttable_dyf.toDF()
# print(facttable_dyf.count())

# pickuplocationsdim
# pickuplocationsdim_dyf = glueContext.create_dynamic_frame.from_options(
#     connection_type="redshift",
#     connection_options={
#         "useConnectionProperties":"true",
#         "connectionName":"myJDBCConn",
#         "dbtable":"pickuplocationsdim_partitioned",
#             # "pickuplocationsdim_partitioned",
#         "redshiftTmpDir":"s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
#     }
#     )
# pickuplocationsdim_df=pickuplocationsdim_dyf.toDF()
# pickuplocationsdim_dyf = pickuplocationsdim_dyf.rename_field("pickup_location_id","pickup_location_id2")
# print(pickuplocationsdim_dyf.count())

# dropofflocationsdim
# dropofflocationsdim_dyf = glueContext.create_dynamic_frame.from_options(
#     connection_type="redshift",
#     connection_options={
#         "useConnectionProperties":"true",
#         "connectionName":"myJDBCConn",
#         "dbtable":"dropofflocationsdi_partitioned",
#         "redshiftTmpDir":"s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
#     }
#     )
# dropofflocationsdim_df=dropofflocationsdim_dyf.toDF()
# dropofflocationsdim_dyf=dropofflocationsdim_dyf.rename_field("dropff_location_id","dropff_location_id2")
# print(dropofflocationsdim_dyf.count())

# final_df=facttable_df.hint('broadcast').join(pickuplocationsdim_df,facttable_df["pickup_location_id"]==pickuplocationsdim_df["pickup_location_id"],'inner')\
# .join(dropofflocationsdim_df,facttable_df["dropff_location_id"]==dropofflocationsdim_df["dropff_location_id"],'inner')\
#                     .select(
#                       'trip_id','vendorid','trip_distance','tpep_pickup_datetime','tpep_dropoff_datetime'
#                       ,facttable_df['pickup_location_id'],'pickup_longitude','pickup_latitude'
#                         ,facttable_df['dropff_location_id'],'dropoff_longitude','dropoff_latitude'
#                       )
# facttable_df (containing 9,800,000 rows) was broadcasted.
# broadcast hash join b/w facttable_df and pickuplocationsdim_df
# shuffle sort merge join b/w o/p of above join and dropofflocationsdim_df


# joining using DynamicFrame. Do not have hint (used shufflesort merge)
# final_df=facttable_dyf.join(paths1=["pickup_location_id"], paths2=["pickup_location_id2"], frame2=pickuplocationsdim_dyf)\
# .join(paths1=["dropff_location_id"], paths2=["dropff_location_id2"], frame2=dropofflocationsdim_dyf)\
# .select_fields(
#     paths=[
#     'trip_id','vendorid','trip_distance','tpep_pickup_datetime',
#                 'pickup_location_id','pickup_longitude','pickup_latitude',
#                 'tpep_dropoff_datetime','dropff_location_id', 'dropoff_longitude','dropoff_latitude'])



# final_df.printSchema()                    
# print(final_df.count())
# print(final_df.rdd.getNumPartitions())
# final_df.show()

# converting th DF to DYF
# final_dyf = DynamicFrame.fromDF(final_df,glueContext,"final_dyf")
# query = "TRUNCATE TABLE analysis_table;"
# glueContext.write_dynamic_frame.from_jdbc_conf(
#     frame = final_dyf, 
#     catalog_connection = "myJDBCConn", 
#     connection_options =  {
#     "dbtable": "analysis_table",
#     "database": "dev",
#     "preactions":query
# },
# redshift_tmp_dir = "s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
# ) 

vendorsdim_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties":"true",
        "connectionName":"myJDBCConn",
        "dbtable":"vendorsdim_partitioned",
        "redshiftTmpDir":"s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
    }
    )

# without hint
vendorsdim_df=vendorsdim_dyf.toDF()
count_df = facttable_df.groupBy('vendorid').count()
final_df = count_df.join(vendorsdim_df,count_df['vendorid']==vendorsdim_df['vendorid'])
# default setings: spark itself used broadcast join

final_df.show()
# Runtime analysis_table (without writing to Redshift): AQE:True   
# When using dynamic frames: broadcast join was not happening
# When Data frames are converted: broadcast join is being used
 
# broadcast is enabled
# 1714487884680: AQE:False SortMergeJoin
# 1714488078571: AQE: True SortMergeJoin

# Now, Increased the autoBroadcastJoinThreshold,
# 1714488723652: AQE: True   broadcast
# 1714489044279: AQE: False SortMergeJoin


job.commit()