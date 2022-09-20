# Databricks notebook source
# MAGIC %md
# MAGIC #Load Data to Azure SQL
# MAGIC 
# MAGIC This requires a new driver to be installed and specific DBR to be used <br>
# MAGIC A knowledge article will be created and sent for this <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC JAR Driver and Setup:
# MAGIC 
# MAGIC Create Cluster to this DB Runtime and Size:
# MAGIC 
# MAGIC 
# MAGIC Install driver from Maven:
# MAGIC 
# MAGIC microsoft/sql-spark-connector: Apache Spark Connector for SQL Server and Azure SQL (github.com)
# MAGIC 
# MAGIC 
# MAGIC This Maven Coordinate:
# MAGIC com.microsoft.azure:spark-mssql-connector_2.12:1.2.0
# MAGIC 
# MAGIC Please follow knowledge article for specifics

# COMMAND ----------

# MAGIC %md
# MAGIC Load Data to pySpark DataFrame
# MAGIC 
# MAGIC Note: This is an example and if data is not loaded to your DBFS, you will have to do this.  If you have data, you can use the data you have

# COMMAND ----------

import pyspark
# Create Dataframe
df = spark.sql("SELECT * FROM default.nyc_taxi")

# COMMAND ----------

# MAGIC %md
# MAGIC # Duplicate the records in the DataFrame
# MAGIC This is just to increase the number of records to do performance testing on the load

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
# MAGIC #Convert Back to PySpark and Load to Temp Table

# COMMAND ----------

# convert back to a spark dataframe
df_large = spark.createDataFrame(df_pandas_duplicated)

# COMMAND ----------

# write to table
print(f'Size of Table written to temp table {df_large.count()}')
df_large.createOrReplaceTempView("scalaWriteTable")

# COMMAND ----------

# MAGIC %scala
# MAGIC val scala_df = table("scalaWriteTable")

# COMMAND ----------

# MAGIC %md
# MAGIC Load Data to Azure SQL

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


