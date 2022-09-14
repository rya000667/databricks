# Databricks notebook source
# MAGIC %md
# MAGIC # Classification Demo to Marketing Team
# MAGIC 
# MAGIC **Objective**: *Demonstrate the use of Principal Components Analysis on a dataset.*
# MAGIC 
# MAGIC Accessing Data in ADLS<br>
# MAGIC Data Exploration<br>
# MAGIC Databricks Inline Graph Tools <br>
# MAGIC Use of SQL <br>
# MAGIC End to End DS Project <br>

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/dsfda.db/ht_agg', True)

# COMMAND ----------

# Load train_test_split
from sklearn.model_selection import train_test_split

# Split into training and test sets
ht_users_df = spark.sql("SELECT device_id, lifestyle, country FROM dsfda.ht_users").toPandas()

ht_users_train_df, ht_users_test_df = train_test_split(ht_users_df, test_size = 0.2, random_state = 42)


# Convert to Spark DataFrames
ht_users_train_sdf = spark.createDataFrame(ht_users_train_df)
ht_users_test_sdf = spark.createDataFrame(ht_users_test_df)

# Create tables for future SQL usage
ht_users_train_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_train")
spark.sql(
  "CREATE TABLE IF NOT EXISTS dsfda.ht_users_train USING DELTA LOCATION '/dsfda/ht_users_train'"
)
ht_users_test_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_test")
spark.sql(
  "CREATE TABLE IF NOT EXISTS dsfda.ht_users_test USING DELTA LOCATION '/dsfda/ht_users_test'"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Explore data
# MAGIC */
# MAGIC SELECT *
# MAGIC FROM dsfda.ht_users_train

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dsfda.ht_users_train

# COMMAND ----------

# MAGIC %sql 
# MAGIC /*
# MAGIC Discover distinct lifestyle types-
# MAGIC */
# MAGIC SELECT DISTINCT lifestyle
# MAGIC FROM dsfda.ht_users_train

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Explore the distribution between all the lifestyles
# MAGIC */
# MAGIC 
# MAGIC SELECT b.lifestyle, b.totalUsers, a.totalUsersAll, b.totalUsers/a.totalUsersAll
# MAGIC FROM (SELECT COUNT(*) AS totalUsersAll FROM dsfda.ht_users_train) a,
# MAGIC (SELECT lifestyle, COUNT(*) AS totalUsers FROM dsfda.ht_users_train GROUP BY lifestyle) b

# COMMAND ----------

# MAGIC %sql 
# MAGIC /*
# MAGIC Explore how many users  are sedentary lifetyle
# MAGIC */
# MAGIC SELECT COUNT(*) totalSedentaryUsers
# MAGIC FROM dsfda.ht_users_train
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle = 'Sedentary'

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Get other details about sedentary users
# MAGIC All sedentary people are from the United States
# MAGIC */
# MAGIC SELECT 
# MAGIC country,
# MAGIC COUNT(*) totalPeople
# MAGIC FROM dsfda.ht_users_train
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle = 'Sedentary'
# MAGIC 
# MAGIC GROUP BY
# MAGIC country

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC country,
# MAGIC COUNT(*) totalPeople
# MAGIC FROM dsfda.ht_users_train
# MAGIC 
# MAGIC GROUP BY
# MAGIC country

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Since 90% of people are not sedentary, assume that all people are not sedentary should yield baseline accuracy of 90%
# MAGIC Results = 88.5%
# MAGIC */
# MAGIC SELECT a.number_correct / b.number_total AS accuracy
# MAGIC FROM (SELECT count(*) AS number_correct 
# MAGIC       FROM dsfda.ht_users_test
# MAGIC       WHERE lifestyle != "Sedentary") a,
# MAGIC      (SELECT count(*) AS number_total FROM dsfda.ht_users_test) b

# COMMAND ----------

# MAGIC %md
# MAGIC # Pull in HT Daily Metrics Data

# COMMAND ----------

'''
dsfda.ht_daily_metrics
'''
# Split into training and test sets
ht_users_metrics_df = spark.sql("SELECT * FROM dsfda.ht_daily_metrics").toPandas()


# COMMAND ----------

ht_users_metrics_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Explore Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.totalUsers, b.lifestyle, a.totalUsers, b.totalUsers/a.totalUsers
# MAGIC FROM (SELECT COUNT(*) AS totalUsers FROM dsfda.ht_daily_metrics) a,
# MAGIC (SELECT COUNT(*) AS totalUsers, lifestyle FROM dsfda.ht_daily_metrics GROUP BY lifestyle) b

# COMMAND ----------

# MAGIC %md
# MAGIC #Format labeled text data to int

# COMMAND ----------

'''
ht_users_train_sdf = spark.createDataFrame(ht_users_train_df)
ht_users_test_sdf = spark.createDataFrame(ht_users_test_df)
'''

from sklearn.preprocessing import LabelEncoder 
le = LabelEncoder()

lifestyle = ht_users_metrics_df['lifestyle']

# fit labels to lifestyle
le.fit(lifestyle)

# get output y array for lifestyle labels
y = le.transform(lifestyle)

# COMMAND ----------

# MAGIC %md
# MAGIC #Perform Logistic Regression
# MAGIC Determine whether or not each user is Sedentary

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

lr = LogisticRegression(max_iter=10000)

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract Relevant Features

# COMMAND ----------

ht_users_metrics_df.display()

# COMMAND ----------

# remove files from '/dsfda/ht_agg_metrics'
dbutils.fs.rm('/dsfda/ht_agg_metrics', True)

# Create tables for future SQL usage
ht_users_train_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_train")
spark.sql(
  "CREATE TABLE IF NOT EXISTS dsfda.ht_users_train USING DELTA LOCATION '/dsfda/ht_users_train'"
)
ht_users_test_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_test")
spark.sql(
  "CREATE TABLE IF NOT EXISTS dsfda.ht_users_test USING DELTA LOCATION '/dsfda/ht_users_test'"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Create new aggregate table
# MAGIC */
# MAGIC DROP TABLE IF EXISTS dsfda.ht_agg_metrics;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS dsfda.ht_agg_metrics
# MAGIC USING DELTA LOCATION '/dsfda/ht_agg_metrics'
# MAGIC SELECT 
# MAGIC device_id,
# MAGIC lifestyle,
# MAGIC MEAN(steps) avg_steps,
# MAGIC MEAN(resting_heartrate) avg_resting_heartrate,
# MAGIC MEAN(active_heartrate) avg_active_heartrate,
# MAGIC MEAN(bmi) avg_bmi,
# MAGIC MEAN(vo2) avg_vo2,
# MAGIC MEAN(workout_minutes) avg_workout_minutes
# MAGIC 
# MAGIC FROM dsfda.ht_daily_metrics
# MAGIC 
# MAGIC GROUP BY
# MAGIC device_id,
# MAGIC lifestyle

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC *
# MAGIC 
# MAGIC FROM dsfda.ht_agg_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Newly Created Table

# COMMAND ----------

# Split into training and test sets
ht_agg_df = spark.sql("SELECT * FROM dsfda.ht_agg_metrics").toPandas()

ht_agg_train_df, ht_agg_test_df = train_test_split(ht_agg_df, test_size = 0.2, random_state = 42)


# Convert to Spark DataFrames
ht_agg_train_sdf = spark.createDataFrame(ht_agg_train_df)
ht_agg_test_sdf = spark.createDataFrame(ht_agg_test_df)

# COMMAND ----------

'''
Features Chosen+
bmi
resting_heartrate
'''

X_1 = ht_users_metrics_df[['resting_heartrate', 'bmi','vo2']]
X_2 = ht_users_metrics_df[['active_heartrate','bmi']]
X_3 = ht_users_metrics_df[['steps',]]

# COMMAND ----------

# MAGIC %md
# MAGIC #Perform Train Test Split

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_1_train, X_1_test, y_1_train, y_1_test = train_test_split(X_1, y)
X_2_train, X_2_test, y_2_train, y_2_test = train_test_split(X_2, y)

# COMMAND ----------

# MAGIC %md
# MAGIC #Fit and Test Models

# COMMAND ----------

lr_1 = LogisticRegression(max_iter=10000)
lr_2 = LogisticRegression(max_iter=10000)

# train models
lr_1.fit(X_1_train, y_1_train)
lr_2.fit(X_2_train, y_2_train)

#score data
print(f'model 1: {lr_1.score(X_1_test, y_1_test)}')
print(f'model 2: {lr_2.score(X_2_test, y_2_test)}')

# COMMAND ----------

# MAGIC %md
# MAGIC #Test Models

# COMMAND ----------

# MAGIC %md
# MAGIC #Perform Label Encoding for Lifestyle Values for aggregate dataframe

# COMMAND ----------

lifestyle_agg = ht_agg_df['lifestyle']

lr=LabelEncoder()

#fit lifestyle labels
le.fit(lifestyle_agg)

# transform lifestyle labels
y_agg = le.transform(lifestyle_agg)


# COMMAND ----------

y_agg

# COMMAND ----------

ht_agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Extrract Features on Aggregated Data

# COMMAND ----------

'''
ht_agg_train_sdf = spark.createDataFrame(ht_agg_train_df)
ht_agg_test_sdf = spark.createDataFrame(ht_agg_test_df)
'''

X_1 = ht_agg_df[['avg_steps']]
X_2 = ht_agg_df[['avg_resting_heartrate','avg_bmi']]
X_3 = ht_agg_df[['avg_vo2', 'avg_workout_minutes']]
X_4 = ht_agg_df[['avg_vo2', 'avg_workout_minutes','avg_steps']]

# COMMAND ----------

# MAGIC %md
# MAGIC #Perform Train Test Split

# COMMAND ----------

X_1_train, X_1_test, y_1_train, y_1_test = train_test_split(X_1, y_agg)
X_2_train, X_2_test, y_2_train, y_2_test = train_test_split(X_2, y_agg)
X_3_train, X_3_test, y_3_train, y_3_test = train_test_split(X_3, y_agg)
X_4_train, X_4_test, y_4_train, y_4_test = train_test_split(X_4, y_agg)


# COMMAND ----------

# MAGIC %md
# MAGIC #Fit and Test Models

# COMMAND ----------

lr_agg_1 = LogisticRegression(max_iter=10000)
lr_agg_2 = LogisticRegression(max_iter=10000)
lr_agg_3 = LogisticRegression(max_iter=10000)
lr_agg_4 = LogisticRegression(max_iter=10000)

# train models
lr_agg_1.fit(X_1_train, y_1_train)
lr_agg_2.fit(X_2_train, y_2_train)
lr_agg_3.fit(X_3_train, y_3_train)
lr_agg_4.fit(X_4_train, y_4_train)

#score data
print(f'model 1: {lr_agg_1.score(X_1_test, y_1_test)}')
print(f'model 2: {lr_agg_2.score(X_2_test, y_2_test)}')
print(f'model 3: {lr_agg_3.score(X_3_test, y_3_test)}')
print(f'model 4: {lr_agg_4.score(X_4_test, y_4_test)}')

# COMMAND ----------

# MAGIC %md
# MAGIC # pyspark ML

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer

# COMMAND ----------

import pyspark.ml

# COMMAND ----------

dir(pyspark.ml)

# COMMAND ----------


ht_agg_spark_df = spark.sql("SELECT * FROM dsfda.ht_agg_metrics")

# label encoding with StringIndexer
indexer = StringIndexer(inputCol="lifestyle", outputCol="lifestyleIndex")
indexed = indexer.fit(ht_agg_spark_df).transform(ht_agg_spark_df)


indexed.display()

# COMMAND ----------


# build vector assembler
assembler = VectorAssembler(inputCols=['avg_vo2', 'avg_workout_minutes','avg_steps'], outputCol="features")

output = assembler.transform(indexed)

final_df = output.select("features", "lifestyleIndex")

# COMMAND ----------

final_df.display(10)

# COMMAND ----------

# create train test split
train, test = final_df.randomSplit([0.7, 0.3], seed=50)

# COMMAND ----------

train.display(10)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

lr_spark = LogisticRegression(labelCol="lifestyleIndex")

lrm = lr_spark.fit(train)

# COMMAND ----------

lrm

# COMMAND ----------

dir(lrm.summary)

# COMMAND ----------

type(lrm)

# COMMAND ----------

lrm.summary.predictions.display()

# COMMAND ----------

lrm.summary.predictions.describe().show()

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

pred_labels = lrm.evaluate(test)

# COMMAND ----------

pred_labels.predictions.display()

# COMMAND ----------

pred_labels.accuracy

# COMMAND ----------

eval = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="lifestyleIndex")

auc = eval.evaluate(pred_labels.predictions)

# COMMAND ----------

auc

# COMMAND ----------

lr_spark_1 = LogisticRegression(maxIter=10000)
lr_spark_2 = LogisticRegression(maxIter=10000)
lr_spark_3 = LogisticRegression(maxIter=10000)
lr_spark_4 = LogisticRegression(maxIter=10000)

# extract features
X_1 = ht_agg_df[['avg_steps']]
X_2 = ht_agg_df[['avg_resting_heartrate','avg_bmi']]
X_3 = ht_agg_df[['avg_vo2', 'avg_workout_minutes']]
X_4 = ht_agg_df[['avg_vo2', 'avg_workout_minutes','avg_steps']]

# train test split
X_1_train, X_1_test, y_1_train, y_1_test = train_test_split(X_1, y_agg)
X_2_train, X_2_test, y_2_train, y_2_test = train_test_split(X_2, y_agg)
X_3_train, X_3_test, y_3_train, y_3_test = train_test_split(X_3, y_agg)
X_4_train, X_4_test, y_4_train, y_4_test = train_test_split(X_4, y_agg)


# train models
lr_agg_1.fit(X_1_train, y_1_train)
lr_agg_2.fit(X_2_train, y_2_train)
lr_agg_3.fit(X_3_train, y_3_train)
lr_agg_4.fit(X_4_train, y_4_train)


# score 

# COMMAND ----------

y_agg

# COMMAND ----------

help(lr_spark_1.fit)

# COMMAND ----------

# train models
lr_spark_1.fit(X_1_train, y_1_train)
lr_spark_2.fit(X_2_train, y_2_train)
lr_spark_3.fit(X_3_train, y_3_train)
lr_spark_4.fit(X_4_train, y_4_train)


# score 

# COMMAND ----------

# MAGIC %md
# MAGIC #MLflow

# COMMAND ----------


