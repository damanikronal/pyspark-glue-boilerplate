from ipaddress import ip_address
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_timestamp
from datetime import datetime, timedelta
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

dateNow = datetime.now()       #Get Current Timestamp
localDate = dateNow + timedelta(hours=6)        #UTC + 7 - 1 Hour Function
currentHour = str(localDate.hour)
currentDay = str(localDate.day)
currentMonth = str(localDate.month)
currentYear = str(localDate.year)
dateForQuery = currentYear + '-' + currentMonth + '-' + currentDay + ' ' + currentHour + '%'
    
mariadbTable= ["table_name"] #Diisi dengan nama table yg mau ditarik datanya

for tableName in mariadbTable:
    
    print('Table Name = ' + tableName +'\n')
    
    #query untuk tarik semua data dengan parameter tanggal
    #query = "(Select * FROM {} where updated_date LIKE {}) tmp".format(tableName, dateForQuery)
    
    #query untuk tarik semua data tanpa parameter tanggal
    query = "(Select * FROM {}) tmp".format(tableName)
    
    print('Trying to pushdown database using Spark.read ')
    
    try:   
    
        jdbcDF = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://" + ip_address + ":" + port_number + "/" + database_name + "?zeroDateTimeBehavior=convertToNull")   \
        .option("dbtable", query)    \
        .option("user", username_db)    \
        .option("password", pass_db)  \
        .load()
    except Exception as ex:
        print('Something went wrong')
        print(ex)
    
    print('Converting data frame to dynamic frame\n')
    
    try:
        dynamicFrame = DynamicFrame.fromDF(jdbcDF, glueContext, 'filtered_dynamic_frame')
    except Exception as ex:
        print('Something went wrong\n')
        print(ex)
        
    jdbcDF.printSchema()
    
    print('put to s3')
    
    try:
        datasink = glueContext.write_dynamic_frame.from_options(
            frame = dynamicFrame, connection_type="s3",
            connection_options = {"path": "s3://" + folder_s3 + "/" + tableName + "/" + currentYear + "/" + currentMonth + "/" + currentDay + "/" + currentHour + "/"},
            format = "parquet")
        print('Transformed data written to S3')
        print('Table ' + tableName + ' written to s3 at s3://' + folder_s3 + "/" + tableName + "/" + currentYear + "/" + currentMonth + "/" + currentDay + "/" + currentHour + "/" + dateForQuery)
    except Exception as ex:
        print('Something went wrong')
        print(ex)
        