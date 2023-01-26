# Databricks notebook source
# MAGIC %md
# MAGIC # Project for Boston Housing Data

# COMMAND ----------

# MAGIC %md
# MAGIC Load Data

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/training/bostonhousing/bostonhousing")

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

housing_df = pd.read_csv("/dbfs/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv", header=0)

# COMMAND ----------

housing_df.head()

# COMMAND ----------

housing_df.columns

housing_df = housing_df.rename(columns = {'Unnamed: 0': 'Unnamed'})

# COMMAND ----------

# MAGIC %md
# MAGIC Create Table to make EDA easier

# COMMAND ----------

# create spark df
housing_spark_df = spark.createDataFrame(housing_df)

housing_spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/bosds/housing-data")

# COMMAND ----------

dbutils.fs.ls("dbfs:/bosds/housing-data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA bosds

# COMMAND ----------

# create sql table
spark.sql(
  "CREATE TABLE bosds.data USING DELTA LOCATION '/bosds/housing-data'"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bosds.data

# COMMAND ----------

# create sql table
spark.sql(
  "CREATE TABLE bosds.dataMnt USING DELTA LOCATION '/dbfs/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv'"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bosds.dataMnt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA sflist

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sflist.data
# MAGIC     USING CSV
# MAGIC     OPTIONS (
# MAGIC     header="true", 
# MAGIC     delimiter=",",
# MAGIC     inferSchema="true",
# MAGIC     path="dbfs:/mnt/training/airbnb-sf-listings.csv"
# MAGIC     )
# MAGIC     

# COMMAND ----------


df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/sf-listings.csv")

# COMMAND ----------

df

# COMMAND ----------

type(df)

# COMMAND ----------

df_spark = spark.createDataFrame(df)

# COMMAND ----------

# housing_spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/bosds/housing-data")

df_spark.write.format("csv").mode("overwrite").option("overwriteSchema", "true").save("/mnt/training/airbnb/cleaned/clean")

# COMMAND ----------

# MAGIC %md
# MAGIC #Explore San Francisco Airbnb Data

# COMMAND ----------

# high level details
sf_airbnb_df = spark.sql("SELECT * FROM sflist.data")

# COMMAND ----------

display(sf_airbnb_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sflist.data

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Total Records
# MAGIC SELECT COUNT(*) AS totalRecords
# MAGIC FROM sflist.data

# COMMAND ----------



# COMMAND ----------


