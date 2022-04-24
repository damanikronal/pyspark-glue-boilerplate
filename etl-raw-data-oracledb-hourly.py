import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime, timedelta
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

dateNow = datetime.now()       #Get Current Timestamp
localDate = dateNow + timedelta(hours=7)        #UTC + 7
currentHour = str(localDate.hour)
currentDay = str(localDate.day)
currentMonth = str(localDate.month)
currentYear = str(localDate.year)

localDateMinus1 = localDate - timedelta(hours=1)
currentHourMinus1 = str(localDateMinus1.hour)
currentDayMinus1 = str(localDateMinus1.day)
currentMonthMinus1 = str(localDateMinus1.month)
currentYearMinus1 = str(localDateMinus1.year)

dateForQuery = currentYear + '-' + currentMonth + '-' + currentDay + ' ' + currentHour + ':00:00'
dateMinus1ForQuery = currentYearMinus1 + '-' + currentMonthMinus1 + '-' + currentDayMinus1 + ' ' + currentHourMinus1 + ':00:00'
#example1 = '2021-12-12 01:00:00'
#example2 = '2021-12-12 00:00:00'

print( dateForQuery + "    " + dateMinus1ForQuery)
transactionTable = ["table_name"] #diisi nama table yang mau ditarik     


for tableName in transactionTable:

    lastUpdateCol = tableName.replace('_FILE','DATE')    
        
    print('Table Name = ' + tableName +'\n')
    query = "(SELECT * FROM SCHEMA.TABLE_NAME)"
    #query = "(SELECT * FROM SCHEMA.TABLE_NAME WHERE date < TO_TIMESTAMP('{}' , 'YYYY-MM-DD HH24:Mi:ss') AND date >= TO_TIMESTAMP('{}' , 'YYYY-MM-DD HH24:Mi:ss'))".format(dateForQuery, dateMinus1ForQuery)
    print(query)
    
    print('spark.read')
    try:   
        jdbcDF = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin://@IPADDR:PORT/SCHEMA")   \
        .option("dbtable", query)    \
        .option("user", username_db)    \
        .option("password", pass_db)  \
        .load()
    
    except Exception as ex:
        print('Something went wrong\n')
        print(ex)
        
    print('Read Data ' + tableName + ' Done\n')    
        
    jdbcDF.show()
    
    convertDF = DynamicFrame.fromDF(jdbcDF, glueContext, 'df_filtered')
    
    try:
        datasink = glueContext.write_dynamic_frame.from_options(
            frame = convertDF, connection_type="s3",
            connection_options = {"path": "s3://" + folder_s3 + "/" + tableName + "/" + currentYear + "/" + currentMonth + "/" + currentDay + "/" + currentHour + "/"},
            format = "parquet")
        print('Transformed data written to S3 \n')
        print("s3://" + folder_s3 + "/" + tableName + "/" + currentYear + "/" + currentMonth + "/" + currentDay + "/" + currentHour + "/\n")
    except Exception as ex:
        print('Something went wrong\n')
        print(ex)
