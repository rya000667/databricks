# Databricks notebook source
# MAGIC %md
# MAGIC #Write Data to Azure SQL
# MAGIC 
# MAGIC Use Cases <br>

# COMMAND ----------

import pyspark
import pyspark.pandas as ps
# Create Dataframe
df = spark.sql("SELECT * FROM default.nyc_taxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT hour_of_day
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

(df.write.partitionBy('hour_of_day') 
.mode("overwrite") 
.format("jdbc") 
.option('url', 'jdbc:sqlserver://testsqlserver03.database.windows.net:1433;database=testdb01') 
.option('dbtable', '[testdb01].[dbo].[nyc_taxi]') 
.option('user', 'notTheAdmin') 
.option('password', 'notThePassword$10') 
.option("truncate","true") 
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
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
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .table('nyc_taxi')
)

# COMMAND ----------

df_ps = ps.DataFrame(df_query)

# COMMAND ----------

dir(df_ps)

# COMMAND ----------

df_ps[df_ps['hour_of_day']==15]

# COMMAND ----------

help(spark.read.table)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write to Delta Lake

# COMMAND ----------

df_query.write.format("delta").mode("overwrite").save()

# COMMAND ----------


