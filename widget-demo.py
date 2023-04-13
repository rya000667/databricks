# Databricks notebook source
# MAGIC %md
# MAGIC For this notebook to work, the below csv file must be persisted to dbfs and a table created as default.baby_names
# MAGIC 
# MAGIC http://health.data.ny.gov/api/views/myeu-hzra/rows.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.baby_names

# COMMAND ----------

df = spark.sql("SELECT * FROM default.baby_names")

# COMMAND ----------

years = spark.sql("select distinct(Year) from default.baby_names").rdd.map(lambda row : row[0]).collect()
years.sort()

# COMMAND ----------

years

# COMMAND ----------

dbutils.widgets.dropdown("year", "2014", [str(x) for x in years])

# COMMAND ----------

display(df.filter(df.Year == getArgument("year")))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.baby_names where Year = getArgument("year")

# COMMAND ----------


