# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Baseline Solution
# MAGIC 
# MAGIC **Objective**: *Develop a baseline solution to a business problem.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises to develop a baseline solution to determine whether health tracker users are from the United States or Canada.

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Summary: Randomly split health tracker users `dsfda.ht_users` into a training set (80 percent of users) and a test set (20 percent of users).

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Load train_test_split
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC 
# MAGIC # Split into training and test sets
# MAGIC ht_users_df = spark.sql("SELECT device_id, country FROM dsfda.ht_users").toPandas()
# MAGIC ht_users_train_df, ht_users_test_df = train_test_split(ht_users_df, test_size = 0.2, random_state = 42)
# MAGIC 
# MAGIC # Convert to Spark DataFrames
# MAGIC ht_users_train_sdf = spark.createDataFrame(ht_users_train_df)
# MAGIC ht_users_test_sdf = spark.createDataFrame(ht_users_test_df)
# MAGIC 
# MAGIC # Create tables for future SQL usage
# MAGIC ht_users_train_sdf.write.format("delta").mode("overwrite").save("/dsfda/ht_users_train")
# MAGIC spark.sql(
# MAGIC   "CREATE TABLE IF NOT EXISTS dsfda.ht_users_train USING DELTA LOCATION '/dsfda/ht_users_train'"
# MAGIC )
# MAGIC ht_users_test_sdf.write.format("delta").mode("overwrite").save("/dsfda/ht_users_test")
# MAGIC spark.sql(
# MAGIC   "CREATE TABLE IF NOT EXISTS dsfda.ht_users_test USING DELTA LOCATION '/dsfda/ht_users_test'"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Summary: Identify what proportion of health tracker users are from the United States and Canada, respectively.

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC percentage from US = total US/ Total Users
# MAGIC percentage from Canada = Total Can/ Total Users
# MAGIC */
# MAGIC SELECT a.country, a.totalUsers, b.totalUsers, a.totalUsers/b.totalUsers AS percentUsersByCountry
# MAGIC FROM (SELECT country, COUNT(*) AS totalUsers FROM dsfda.ht_users GROUP BY country) a,
# MAGIC (SELECT COUNT(*) totalUsers FROM dsfda.ht_users) b

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Summary: Apply a most-common case baseline solution to the test set and save to a new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT device_id, "United States" as predicted_country
# MAGIC FROM dsfda.ht_users_test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Summary: Evaluate the baseline solution's accuracy on the test data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT a.number_correct / b.number_total AS accuracy
# MAGIC FROM (SELECT count(*) AS number_correct 
# MAGIC       FROM dsfda.ht_users_test
# MAGIC       WHERE country = "United States") a,
# MAGIC      (SELECT count(*) AS number_total FROM dsfda.ht_users_test) b

# COMMAND ----------

# MAGIC %md
# MAGIC Great job completing the Baseline Solution lab! Continue on with the lesson to learn about measuring solutions in real-world settings.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
