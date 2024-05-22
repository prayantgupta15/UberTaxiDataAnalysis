import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *


from pyspark.sql.types import StructType,StructField,IntegerType,StringType



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled','false')



def writeToRedshift(dyf,tableName):
    query='truncate table {} ;'.format(tableName)
    glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection = "myJDBCConn", 
    connection_options =  {
    "preactions":query,
    "dbtable": tableName,
    "database": "dev",
},
redshift_tmp_dir = "s3://aws-glue-assets-079448565720-us-west-1/connectionTempDir/"
    )


# add logic for previous dates

dyf = glueContext.create_dynamic_frame_from_catalog(
    database="uberdb",
    table_name="uberdatahive"
    ,push_down_predicate = "date>=28",
     additional_options={
        "catalogPartitionPredicate":"year='2023' and month='5'"
    }
    # ,  
    # (for now commenting to load all dates data)
    # transformation_ctx="dyf"
    # to enable the bookmark feature
    )


df = dyf.toDF()
# print(df.count())
# ===============================================================
# Original DF
print('Original DF:')

df = df.withColumn('trip_id',monotonically_increasing_id()+1)\
.withColumn('VendorID',col('VendorID').cast('integer'))\
.withColumn('payment_type',col('payment_type').cast('integer'))\
.withColumn("tpep_pickup_datetime",to_timestamp(col("tpep_pickup_datetime"))) \
.withColumn("tpep_dropoff_datetime",to_timestamp("tpep_dropoff_datetime")) \
.withColumn("passenger_count",col("passenger_count").cast('integer'))\
.withColumn("trip_distance",col("trip_distance").cast('double'))\
.withColumn("pickup_longitude",col("pickup_longitude").cast('double'))\
.withColumn("pickup_latitude",col("pickup_latitude").cast('double'))\
.withColumn("dropoff_longitude",col("dropoff_longitude").cast('double'))\
.withColumn("dropoff_latitude",col("dropoff_latitude").cast('double'))\
.withColumn("fare_amount",col("fare_amount").cast('double'))\
.withColumn("extra",col("extra").cast('double'))\
.withColumn("mta_tax",col("mta_tax").cast('double'))\
.withColumn("tolls_amount",col("tolls_amount").cast('double'))\
.withColumn("improvement_surcharge",col("improvement_surcharge").cast('double'))\
.withColumn("total_amount",col("total_amount").cast('double'))\
.withColumn("tip_amount",col("tip_amount").cast('double'))\
.withColumn("pickup_location_id",col("trip_id").alias('pickup_location_id'))\
.withColumn('RatecodeID',col('RatecodeID').cast('integer'))\
.select('*',col('trip_id').alias('dropff_location_id'))

# Fact table DF
# ===============================================================
print('FactTable DF:')
fact_table_df = df.select('trip_id','VendorID','passenger_count','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance',
                          'RatecodeID',
                          "payment_type",
                          'fare_amount','extra',
                          'mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount',
                          'pickup_location_id','dropff_location_id'                         
                          )
# fact_table_df.show(2,False)
fact_table_df.printSchema()
fact_table_dyf = DynamicFrame.fromDF(fact_table_df,glueContext,"fact_table_dyf")
writeToRedshift(fact_table_dyf,"FactTable_partitioned")

# fact_table_df.createOrReplaceTempView('fact_table_partitioned')

# spark.sql("""
# select VendorID,count(*) from fact_table_df group by VendorID;
# """
# ).show()
# ===============================================================

# # dim tables
# # ===============================================================
print('PaymentType DF:')
payment_data = [
[1, "Credit card"],
[2, "Cash"],
[3, "No charge"],
[4, "Dispute"],
[5, "Unknown"],
[6, "Voided trip"]
]

mySchema = StructType(
    [
        StructField('payment_type',IntegerType(),True),
        StructField('payment_type_name',StringType(),True)
    ]
)
payment_data_df = spark.createDataFrame(payment_data,mySchema)
payment_data_df.printSchema()
payment_data_dyf = DynamicFrame.fromDF(payment_data_df,glueContext,"payment_data_dyf")
writeToRedshift(payment_data_dyf,"PaymentTypesDim_partitioned")


print('Pickup location DF:')
pickup_location_df = df.select('pickup_location_id','pickup_longitude','pickup_latitude')
pickup_location_df.printSchema()
pickup_location_dyf = DynamicFrame.fromDF(pickup_location_df,glueContext,"pickup_location_dyf")
writeToRedshift(pickup_location_dyf,"PickupLocationsDim_partitioned")


print("Drop location DF:")
dropoff_location_df = df.select('dropff_location_id','dropoff_longitude','dropoff_latitude')
dropoff_location_df.printSchema()
dropoff_location_dyf = DynamicFrame.fromDF(dropoff_location_df,glueContext,"dropoff_location_dyf")
writeToRedshift(dropoff_location_dyf,"DropOffLocationsDi_partitioned")


print("Date DF:")
date_df = df.select(
    col('tpep_pickup_datetime').alias('datekey')).coalesce(44).distinct()\
.union(df.select('tpep_dropoff_datetime').coalesce(44).distinct())

date_df=date_df.distinct()
# date_df=date_df.dropDuplicates()

date_df = date_df.select(
    'datekey',

    to_date('datekey','yyyy-MM-dd')
    # expr("date_format(tpep_pickup_datetime,'yyyy-MM-dd')")
    .alias('date'))\
.withColumn('year_yy', expr("date_format(datekey,'yy')"))\
.withColumn('year_yyyy', expr("date_format(datekey,'y')"))\
.withColumn('month',expr("date_format(datekey,'MMMM')"))

date_df.printSchema()
# date_df.show()
date_dyf = DynamicFrame.fromDF(date_df,glueContext,"date_dyf")
writeToRedshift(date_dyf,"DateDim_partitioned")


rate_code_data = [
[1,"Standard rate"],
[2,"JFK"],
[3,"Newark"],
[4,"Nassau or Westchester"],
[5,"Negotiated fare"],
[6,"Group ride"]
]
                   
rate_code_schema = StructType([
    StructField('RatecodeID',StringType()),
    StructField('RatecodeName',StringType())
])

rate_code_df = spark.createDataFrame(rate_code_data,rate_code_schema)
rate_code_df.printSchema()
rate_code_dyf = DynamicFrame.fromDF(rate_code_df,glueContext,"rate_code_dyf")
writeToRedshift(rate_code_dyf,"RateCodesDim_partitioned")

vendor_data = [[1, "Creative Mobile Technologies"],[2,"VeriFone Inc."]]
vendor_schema = StructType([
    StructField("VendorID",IntegerType()),
    StructField("VendorName",StringType())
    ])
vendor_df = spark.createDataFrame(vendor_data,vendor_schema)
vendor_dyf = DynamicFrame.fromDF(vendor_df,glueContext,"vendor_dyf")
writeToRedshift(vendor_dyf,"VendorsDim_partitioned")

# ===============================================================
job.commit()





# vendor_data = [[1, "Creative Mobile Technologies"],[2,"VeriFone Inc."]]
# vendor_schema = StructType([
#     StructField("VendorID",IntegerType()),
#     StructField("VendorName",StringType())
#     ])
# vendor_df = spark.createDataFrame(vendor_data,vendor_schema)
# # vendor_dyf = DynamicFrame.fromDF(vendor_df,glueContext,"vendor_dyf")

# # join_df = fact_table_df.hint('broadcast').join(vendor_df,fact_table_df['VendorID']==vendor_df['VendorID'],'inner').select('trip_id',fact_table_df['VendorID'],'VendorName')
# # AQE:False broadcasted the bigger table jr_8cb0a81ae7acf4e493028b511f2df5b68f1271646c6e7e98b572c2d18b715e38


# # join_df = fact_table_df.join(vendor_df,fact_table_df['VendorID']==vendor_df['VendorID'],'inner').select('trip_id',fact_table_df['VendorID'],'VendorName')
# # AQE:False used SortMergeJoin (default) jr_2183922f24d9661e7ef8bebe11a1015d6d313cd816788dd39c4e0879bcbc41f7

# # join_df = vendor_df.hint('broadcast').join(fact_table_df,fact_table_df['VendorID']==vendor_df['VendorID'],'inner').select('trip_id',fact_table_df['VendorID'],'VendorName')
# fact_table_df.groupBy('VendorID').count()
# # AQE:False broadcasted the small table jr_8cdd67ca9c944237372d6abc3800d051592e5d850688857e1b46727a11d21a58

# join_df.show()

# job.commit()