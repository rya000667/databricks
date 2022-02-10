# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Descriptive Statistics
# MAGIC 
# MAGIC **Objective**: *Use descriptive statistics to gather information about a data set.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises to calculate various summary statistics of the `dsfda.ht_daily_metrics` data set.

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Measures of Central Tendency

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Exercise 1: Mean
# MAGIC 
# MAGIC Use the `AVG` SQL function to calculate the average resting heartrate, as an integer, for each lifestyle.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If needed, check out the [Spark SQL documentation on `avg`](https://spark.apache.org/docs/3.0.1/api/sql/index.html#avg).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(AVG(resting_heartrate) AS int) as avg_resting_heartrate, 
# MAGIC lifestyle
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC GROUP BY lifestyle

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Exercise 2: Median
# MAGIC 
# MAGIC Use the `percentile` SQL function to calculate the median resting heartrate, as an integer, for the "Athlete" lifestyle.
# MAGIC 
# MAGIC Note that this code block takes longer to run; this is because calculating the median requires shuffling across worker nodes.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If needed, check out the [Spark SQL documentation on `percentile`](https://spark.apache.org/docs/3.0.1/api/sql/index.html#percentile).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(percentile(resting_heartrate, .5) AS int)
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC WHERE lifestyle = 'Athlete'

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Exercise 3: Mode
# MAGIC 
# MAGIC Calculate the mode, as an integer, of the resting heartrate for each lifestyle.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Cast the heartrate to the nearest integer first. You can use the Spark SQL documentation on `cast` for reference.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Try to find the `mode` Spark SQL function in the documentation on your own.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS count, CAST(resting_heartrate AS int) AS recast_resting_heartrate
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC GROUP BY recast_resting_heartrate
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Measures of Dispersion

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Exercise 4: Standard Deviation
# MAGIC 
# MAGIC Calculate the standard deviation of the resting heartrate for each lifestyle.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Cast the heartrate to the nearest integer first.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the Spark SQL documentation to determine which function computes the standard deviation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC SELECT CAST(STD(resting_heartrate) AS int) as std_resting_heartrate, lifestyle
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC GROUP BY lifestyle

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Exercise 5: Interquartile Range
# MAGIC 
# MAGIC Calculate the interquartile range of resting heartrate, as an integer, for each lifestyle.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Recall that the interquartile range is the difference between the 75th percentile and the 25th percentile.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Refer to previous exercises in this lab to determine which Spark SQL function can be used to compute the value at a given percentile.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC lifestyle,
# MAGIC CAST(percentile(resting_heartrate, .75) AS int) - CAST(percentile(resting_heartrate, .25) AS int)
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC GROUP BY lifestyle

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations! You've completed the Descriptive Statistics Lab.
# MAGIC 
# MAGIC If you have any trouble with any of this course's labs, be sure to check out the solutions to the labs in the Solutions folder.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
