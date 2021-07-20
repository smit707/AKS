# IMPORTING LIBRARIES

# Importing python modules
from datetime import datetime

# Importing pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

# Importing glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initializing session and context
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# Parameters
glue_db = "virtual-database"
glue_tbl = "input"
s3_write_path = "s3://sample-bucket-glue/output"

#-------------------------------#
### EXTRACTING ( READ DATA )
#-------------------------------#

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

# Converting dynamic frame to data frame to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()


#-------------------------------#
### TRANSFORM ( MODIFY DATA )
#-------------------------------#


data_frame_aggregated = data_frame.groupby("VendorID").agg(
    f.mean(f.col("total_amount")).alias('avg_total_amount'),
    f.mean(f.col("trip_name")).alias('avg_trip_time'),
)

data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("avg_total_amount"))


#-------------------------------#
### LOAD ( WRITE DATA )
#-------------------------------#

# Creating 1 parition because we have less amount of data
data_frame_aggregated = data_frame_aggregated.repartition(1)

# Converting back into dynamic frame
data_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

# Writing data back to S3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,

    },
    format = "csv"
)
