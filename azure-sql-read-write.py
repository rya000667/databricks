# Databricks notebook source
# MAGIC %md
# MAGIC #Write Data to Azure SQL
# MAGIC 
# MAGIC Use Cases <br>

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.sqlserver.jdbc.spark

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST JAR;

# COMMAND ----------

import pyspark
# import pyspark.pandas as ps
# Create Dataframe
df = spark.sql("SELECT * FROM default.nyc_taxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.nyc_taxi

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM default.nyc_taxi

# COMMAND ----------

# choose hour of day to partition
dir(ps)

# COMMAND ----------

df2 = ps.sql("SELECT * FROM default.nyc_taxi")

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Data to Azure SQL Database

# COMMAND ----------

# MAGIC %md
# MAGIC # Convert a Spark DataFrame to Scala DataFrame

# COMMAND ----------

# create a temp view with python spark dataframe
df.createOrReplaceTempView("TempView")

# COMMAND ----------

# MAGIC %scala
# MAGIC val scala_df = table("TempView")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.types._
# MAGIC import com.microsoft.sqlserver.jdbc.spark
# MAGIC 
# MAGIC val server_name = "jdbc:sqlserver://testsqlserver03.database.windows.net"
# MAGIC val database_name = "testdb01"
# MAGIC val url = "jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01"
# MAGIC val user = "notTheAdmin"
# MAGIC val password = "notThePassword$10"
# MAGIC 
# MAGIC 
# MAGIC val tableName1 = "nyc_taxi"
# MAGIC 
# MAGIC scala_df.write
# MAGIC .format("com.microsoft.sqlserver.jdbc.spark")
# MAGIC .mode("append")
# MAGIC .option("truncate" ,true)
# MAGIC .option("url", url)
# MAGIC .option("dbtable", tableName1)
# MAGIC .option("user", user)
# MAGIC .option("password", password)
# MAGIC .option("reliabilityLevel", "BEST_EFFORT")
# MAGIC .option("tableLock", "false")
# MAGIC .option("batchsize", "1048576")
# MAGIC .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC /*
# MAGIC val bulkCopyConfig = Config(Map(
# MAGIC   "url"               -> "jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01",
# MAGIC   "databaseName"      -> "testdb01",
# MAGIC   "user"              -> "notTheAdmin",
# MAGIC   "password"          -> "notThePassword$10",
# MAGIC   "dbTable"           -> "dbo.Clients",
# MAGIC   "bulkCopyBatchSize" -> "2500",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "600"
# MAGIC ))
# MAGIC 
# MAGIC 
# MAGIC scala_df.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)
# MAGIC 
# MAGIC scala_df.bulkCopyToSqlDB(bulkCopyConfig)
# MAGIC */

# COMMAND ----------

(df.write.partitionBy('hour_of_day') 
.mode("append") 
.format("com.microsoft.sqlserver.jdbc.spark") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
# .option("driver", "com.microsoft.sqlserver.jdbc.spark") 
.save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # To Do
# MAGIC Check how the records were written on the SQL side <br>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Read the Data From SQL Server

# COMMAND ----------

# Create the DataFrame.
df_read = (spark
  .read
  .format("jdbc") 
  .option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') \
  .option('dbtable', '[testdb01].[dbo].[nyc_taxi]') \
  .option('user', 'notTheAdmin') \
  .option('password', 'notThePassword$10') \
  .option("truncate","true") \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()
)

# COMMAND ----------

df_read.display()

# COMMAND ----------

help(pyspark.pandas.sql)

# COMMAND ----------

# Create the DataFrame.
df_query = (spark
  .read
  .format("jdbc") 
  .option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') \
  .option('user', 'notTheAdmin') \
  .option('password', 'notThePassword$10') \
  .option("truncate","true") \
  .option("driver", "com.microsoft.sqlserver.jdbc.spark") \
  .table('nyc_taxi')
)

# COMMAND ----------

df_ps = ps.DataFrame(df_query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write to Delta Lake

# COMMAND ----------

df_query.write.format("delta").mode("overwrite").save("/tmp/delta-table")

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/delta-table

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS NYC_WRITE_TEST""")


spark.sql("""
CREATE TABLE NYC_WRITE_TEST
USING DELTA LOCATION '{}'
""".format('/tmp/delta-table'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM NYC_WRITE_TEST

# COMMAND ----------

# MAGIC %md
# MAGIC # Duplcate the records in the DataFrame
# MAGIC This is to get the same number of records as STIP
# MAGIC 
# MAGIC nyc-taxi dataset initial = 11,734 records
# MAGIC STIP one time load = 21,751,630
# MAGIC 
# MAGIC nyc-dataset will have to be duplicated 1,853.7268 times

# COMMAND ----------

df.count()

# COMMAND ----------

import pandas as pd

df_pandas = df.toPandas()

# COMMAND ----------

df_pandas_duplicated = pd.concat([df_pandas]*1854*3 , ignore_index=True)

# COMMAND ----------

len(df_pandas_duplicated)

# COMMAND ----------

# MAGIC %md
# MAGIC # The number of records in the dataframe is now the same as the number in STIP
# MAGIC Keep in mind the counts of columns are different, but this can be adjusted as well

# COMMAND ----------

# convert back to a spark dataframe
df_large = spark.createDataFrame(df_pandas_duplicated)

# COMMAND ----------

# show that the records are the same
df_large.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort the DataFrame

# COMMAND ----------

sorted_df = df_large.sort(df_large.hour_of_day.desc())

# COMMAND ----------

(df_large.write
.format("com.microsoft.sqlserver.jdbc.spark")
.mode("append") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
.save()
)

# COMMAND ----------

# same as above, but with bulk load

(df_large.write.partitionBy('hour_of_day') 
.mode("append") 
.format("jdbc") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
.option("bulkCopyBatchSize",100000)
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
.save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Table as Baseline

# COMMAND ----------

df_large.count()

# COMMAND ----------

df_large.write.format("delta").partitionBy('hour_of_day').mode("overwrite").save("/tmp/test-table")

# COMMAND ----------

# write to delta table as a baseline
spark.sql("""
DROP TABLE IF EXISTS NYC_WRITE_TEST""")


# COMMAND ----------

spark.sql("""
CREATE TABLE NYC_WRITE_TEST
USING DELTA LOCATION '{}'
""".format('/tmp/test-table'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM NYC_WRITE_TEST

# COMMAND ----------

# sbulk load with different values and partition by removed append changed to overwrite

(df_large.write
.mode("overwrite") 
.format("jdbc") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
.option("bulkCopyBatchSize",1048576)
.option("bulkCopyTableLock", "true")
.option("bulkCopyTimeout", "648000")
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
.save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Following MS Best Practices Video

# COMMAND ----------

spark.conf.set("spark.sql.suffle.partitions",4)

# COMMAND ----------

# df_large.coalesce(21)
repartitioned = df_large.coalesce(21)

# COMMAND ----------

repartitioned.rdd.getNumPartitions()

# COMMAND ----------

repartitioned.count()

# COMMAND ----------

# Load into heap first
# use spark driver
# use append mode
# use tablelock true
# use 100k for batch size

(repartitioned.write 
.mode("append") 
.format("jdbc") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]')  
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("reliabilityLevel", "BEST_EFFORT")
.option("bulkCopyTableLock", "true")
.option("truncate","true") 
.option("bulkCopyBatchSize",100000)
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
.save()
)

# COMMAND ----------

help(df_large.sort)

# COMMAND ----------

# sbulk load with different values and partition by removed append changed to overwrite

(sorted_df.write
.mode("append") 
.format("jdbc") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
.option("bulkCopyBatchSize",100000)
.option("reliabilityLevel", "BEST_EFFORT")
.option("bulkCopyTableLock", "true")
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
.save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write large data with Scala

# COMMAND ----------

df_large.count()

# COMMAND ----------


df_large['passengerCount'].astype('INT')

# COMMAND ----------

pan_df = df_large.toPandas()

# COMMAND ----------

pan_df['passengerCount'].astype(int)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

dir(explore_t)

# COMMAND ----------

help(df_large.schema)

# COMMAND ----------

help(pyspark.sql.types.StructType)

# COMMAND ----------

import pyspark.sql.types

mySchema = StructType([StructField("passengerCount", IntegerType(), True),
            StructField("tripDistance", DoubleType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("month_num", IntegerType(), True),
            StructField("normalizeHolidayName", StringType(), True),
            StructField("isPaidTimeOff", BooleanType(), True),
            StructField('isPaidTimeOff', BooleanType(), True),
            StructField('snowDepth',  DoubleType(), True),
            StructField('precipTime',  DoubleType(), True),
            StructField('precipDepth', DoubleType(), True),
            StructField('temperature',  DoubleType(), True),
            StructField('totalAmount',  DoubleType(), True)
           ])

# COMMAND ----------

df_large._schema = mySchema

# COMMAND ----------

df_large

# COMMAND ----------

# write to table
print(f'Size of Table written to temp table {df_large.count()}')
df_large.createOrReplaceTempView("scalaWriteTable")

# COMMAND ----------

# MAGIC %scala
# MAGIC val scala_df = table("scalaWriteTable")

# COMMAND ----------

# MAGIC %scala
# MAGIC scala_df.withColumn("passengerCount", $"passengerCount".cast("int"))

# COMMAND ----------

# MAGIC %scala
# MAGIC scala_df

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.types._
# MAGIC import com.microsoft.sqlserver.jdbc.spark
# MAGIC 
# MAGIC val server_name = "jdbc:sqlserver://testsqlserver03.database.windows.net"
# MAGIC val database_name = "testdb01"
# MAGIC val url = "jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01"
# MAGIC val user = "notTheAdmin"
# MAGIC val password = "notThePassword$10"
# MAGIC 
# MAGIC 
# MAGIC val tableName1 = "nyc_taxi_4"
# MAGIC 
# MAGIC scala_df.write
# MAGIC .format("com.microsoft.sqlserver.jdbc.spark")
# MAGIC .mode("overwrite")
# MAGIC .option("truncate" ,true)
# MAGIC .option("url", url)
# MAGIC .option("dbtable", tableName1)
# MAGIC .option("user", user)
# MAGIC .option("password", password)
# MAGIC .option("reliabilityLevel", "BEST_EFFORT")
# MAGIC .option("tableLock", "true")
# MAGIC .option("batchsize", "1048576")
# MAGIC .save()

# COMMAND ----------


