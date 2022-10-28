# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Hypothesis Testing (25 mins)
# MAGIC 
# MAGIC **Objective**: *Perform a two-sample, two-sided t-test to compare sample means.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises to perform a t-test.
# MAGIC 
# MAGIC We want to determine whether the population mean of daily steps taken for the athlete health tracker users is equal to the population mean of daily steps taken for the cardio enthusiast health tracker users.
# MAGIC 
# MAGIC The null and alternative hypotheses for this test are:
# MAGIC 
# MAGIC * H<sub>0</sub> = the mean of daily steps taken for athlete users is equal to the mean of daily steps taken for cardio enthusiast users.
# MAGIC * H<sub>1</sub> = the mean of daily steps taken for athlete users is not equal to the mean of daily steps taken for cardio enthusiast users.

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls dbfs:/user/hive

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/dsfda.db/ht_agg/',True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/dsfda.db/ht_agg')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC DISTINCT lifestyle
# MAGIC FROM ht_daily_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC DISTINCT lifestyle
# MAGIC 
# MAGIC FROM dsfda.ht_agg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Compute the sample mean of daily steps taken for sedentary users and cardio enthusiast users.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC lifestyle,
# MAGIC avg(steps) sample_mean_steps
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle IN('Cardio Enthusiast', 'Sedentary', 'Athlete')
# MAGIC 
# MAGIC GROUP BY
# MAGIC lifestyle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC Compute the sample variance of daily steps taken for sedentary users and cardio enthusiast users.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC lifestyle,
# MAGIC variance(steps) var_steps
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle IN('Cardio Enthusiast', 'Sedentary', 'Athlete')
# MAGIC 
# MAGIC GROUP BY
# MAGIC lifestyle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC Compute the sample size for sedentary users and cardio enthusiast users.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC lifestyle,
# MAGIC count(steps) total_samples
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle IN('Cardio Enthusiast', 'Sedentary', 'Athlete')
# MAGIC 
# MAGIC GROUP BY
# MAGIC lifestyle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4
# MAGIC Compute the T-statistic using the sample statistics.

# COMMAND ----------

# TODO
from math import sqrt

athlete_mean = 11001.597413366928
athlete_variance = 9623550.234400436
athlete_size = 313535

cardio_mean = 13235.376990421259
cardio_variance = 13171492.338618575
cardio_size = 388360

test_statistic = (athlete_mean - cardio_mean) / sqrt((athlete_variance / athlete_size) + (cardio_variance / cardio_size))
print(f"T-statistic = {test_statistic}")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5
# MAGIC Compute the degrees of freedom using the sample statistics.

# COMMAND ----------

# TODO
df_numerator = ((athlete_variance / athlete_size) + (cardio_variance / cardio_size))**2
df_denominator = (athlete_variance / athlete_size)**2 / (athlete_size - 1) + ((cardio_variance / cardio_size)**2 / (cardio_size - 1))
df = df_numerator/df_denominator
print(f"Degrees-of-freedom = {df}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 6
# MAGIC 
# MAGIC Compute the p-value for this T-test by passing in the `test_statistic` and the `df` to `t.cdf()`.

# COMMAND ----------

# TODO
from scipy.stats import t
p_value = t.cdf(test_statistic, df)
print(f"p-value = {p_value}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 7
# MAGIC 
# MAGIC Determine whether we should reject the null hypothesis. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Use a significance level of 0.05.

# COMMAND ----------

print('The p-value is {} which is less than .05, therefore we reject the null hypothesis'.format(p_value))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8
# MAGIC 
# MAGIC Phew! That was a lot of work to answer a simple question.
# MAGIC 
# MAGIC Luckily, Python's `scipy` module makes this process a bit easier than it already has.
# MAGIC 
# MAGIC Check out the demonstration below showing how to perform this same test in a single step only using Python.

# COMMAND ----------

from scipy.stats import ttest_ind

athlete_daily_steps = spark.sql("SELECT steps FROM dsfda.ht_daily_metrics WHERE lifestyle = 'Athlete'").toPandas()["steps"]
cardio_daily_steps = spark.sql("SELECT steps FROM dsfda.ht_daily_metrics WHERE lifestyle = 'Cardio Enthusiast'").toPandas()["steps"]

ttest_ind(athlete_daily_steps, cardio_daily_steps, equal_var = False)

# COMMAND ----------


athlete_daily_steps = spark.sql("SELECT steps FROM dsfda.ht_daily_metrics WHERE lifestyle = 'Athlete'").toPandas()
cardio_daily_steps = spark.sql("SELECT steps FROM dsfda.ht_daily_metrics WHERE lifestyle = 'Cardio Enthusiast'").toPandas()



# COMMAND ----------

athlete_daily_steps = spark.sql("SELECT steps FROM dsfda.ht_daily_metrics WHERE lifestyle = 'Athlete'")

# COMMAND ----------

athlete_daily_steps.toPandas()['steps']

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that the *same test statistic* and *same p-value* were calculated with far less code!
# MAGIC 
# MAGIC While it's good to understand how these `scipy` tools work, it's good practice and efficient to use them as much as possible.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
