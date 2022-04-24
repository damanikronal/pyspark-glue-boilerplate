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
from pyspark.sql.functions import when, col, to_timestamp, lit, monotonically_increasing_id, concat, concat_ws, sum

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
#example1 = '2014-01-01 23:00:00'
#example2 = '2014-01-01 00:00:00'

#FACT TABLE
#===================================================================================================================================================================================================================================================================

df_data = spark.read.parquet("s3://" + folder_s3 + "/" + currentYear + "/" + currentMonth + "/20/9" + "/*.parquet")   
df_dataView = df_data.createOrReplaceTempView("dataView")

df_transaksi = spark.read.parquet("s3://" + folder_s3 + "/" + currentYear + "/" + currentMonth + "/20/9" + "/*.parquet")
df_transaksiView = df_transaksi.createOrReplaceTempView("transaksiView")

print('Read Finish.. Start sql')

df_fact_wbi = spark.sql("""SELECT A.id AS id, A.master_date, 
B.transaksi_no, B.transaksi_name
FROM dataView A
LEFT JOIN transaksiView B ON A.id = B.id""") 

df_fact_table.show(2)

try:
    converterdDF = DynamicFrame.fromDF(df_fact_table, glueContext, 'filtered_dynamic_frame')
except Exception as ex:
    print('Something went wrong\n')
    print(ex)
       
print('try to put to s3')
   
try:
    datasink = glueContext.write_dynamic_frame.from_options(
        frame = converterdDF, connection_type="s3",
        connection_options = {"path": "s3://" + folder_fact_table + "/" + currentYear + "/" + currentMonth + "/" + currentDay + "/" + currentHour + "/"},
        format = "parquet")
    print('Transformed data written to S3')
except Exception as ex:
    print('Something went wrong')
    print(ex)
