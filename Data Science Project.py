# Databricks notebook source
# MAGIC %md
# MAGIC d-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Science Project
# MAGIC 
# MAGIC **Objective**: *Design, complete, and assess a common data science project.*
# MAGIC 
# MAGIC In this lab, you will use the data science process to design, build, and assess a common data science project.

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/dsfda.db/ht_agg', True)

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC ## Project Details
# MAGIC 
# MAGIC In recent months, our health tracker company has noticed that many customers drop out of the sign-up process when they have to self-identify their exercise lifestyle (`ht_users.lifestyle`) – this is especially true for those with a "Sedentary" lifestyle. As a result, the company is considering removing this step from the sign-up process. However, the company knows this data is valuable for targeting introductory exercises and they don't want to lose it for customers that sign up after the step is removed.
# MAGIC 
# MAGIC In this data science project, our business stakeholders are interested in identifying which customers have a sedentary lifestyle – specifically, they want to know if we can correctly identify whether somebody has a "Sedentary" lifestyle at least 95 percent of the time. If we can meet this objective, the organization will be able to remove the lifestyle-specification step of the sign-up process *without losing the valuable information provided by the data*.
# MAGIC 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> There are no solutions provided for this project. You will need to complete it independently using the guidance detailed below and the previous labs from the project.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Load train_test_split
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC 
# MAGIC # Split into training and test sets
# MAGIC ht_users_df = spark.sql("SELECT device_id, lifestyle, country FROM dsfda.ht_users").toPandas()
# MAGIC 
# MAGIC ht_users_train_df, ht_users_test_df = train_test_split(ht_users_df, test_size = 0.2, random_state = 42)
# MAGIC 
# MAGIC 
# MAGIC # Convert to Spark DataFrames
# MAGIC ht_users_train_sdf = spark.createDataFrame(ht_users_train_df)
# MAGIC ht_users_test_sdf = spark.createDataFrame(ht_users_test_df)
# MAGIC 
# MAGIC # Create tables for future SQL usage
# MAGIC ht_users_train_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_train")
# MAGIC spark.sql(
# MAGIC   "CREATE TABLE IF NOT EXISTS dsfda.ht_users_train USING DELTA LOCATION '/dsfda/ht_users_train'"
# MAGIC )
# MAGIC ht_users_test_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/dsfda/ht_users_test")
# MAGIC spark.sql(
# MAGIC   "CREATE TABLE IF NOT EXISTS dsfda.ht_users_test USING DELTA LOCATION '/dsfda/ht_users_test'"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Explore data
# MAGIC */
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
# MAGIC -sandbox
# MAGIC 
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Summary: 
# MAGIC * Specify the data science process question. 
# MAGIC * Indicate whether this is framed as a supervised learning or unsupervised learning problem. 
# MAGIC * If it is supervised learning, indicate whether the problem is a regression problem or a classification problem.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** When we are interested in predicting something, we are usually talking about a supervised learning problem.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Summary: 
# MAGIC 
# MAGIC * Specify the data science objective. 
# MAGIC * Indicate which evaluation metric should be used to assess the objective.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Remember, the data science objective needs to be measurable.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Summary:
# MAGIC * Design a baseline solution.
# MAGIC * Develop a baseline solution – be sure to split data between training for development and test for assessment.
# MAGIC * Assess your baseline solution. Does it meet the project objective? If not, use it as a threshold for further development.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Recall that baseline solutions are meant to be easy to develop.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Summary: 
# MAGIC * Design the machine learning solution, but do not yet develop it. 
# MAGIC * Indicate whether a machine learning model will be used. If so, indicate which machine learning model will be used and what the label/output variable will be.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider solutions that align with the framing you did in Exercise 1.

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
# MAGIC -sandbox
# MAGIC ## Exercise 5
# MAGIC 
# MAGIC Summary: 
# MAGIC * Explore your data. 
# MAGIC * Specify which tables and columns will be used for your label/output variable and your feature variables.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider aggregating features from other tables.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC ## Exercise 6
# MAGIC 
# MAGIC Summary: 
# MAGIC * Prepare your modeling data. 
# MAGIC * Create a customer-level modeling table with the correct output variable and features. 
# MAGIC * Finally, split your data between training and test sets. Make sure this split aligns with that of your baseline solution.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider how to make the data split reproducible.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC ## Exercise 7
# MAGIC 
# MAGIC Summary: 
# MAGIC * Build the model specified in your answer to Exercise 4. 
# MAGIC * Be sure to use an evaluation metric that aligns with your specified objective.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This evaluation metric should align with the one used in your baseline solution.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC ## Exercise 8
# MAGIC 
# MAGIC Summary: 
# MAGIC * Assess your model against the overall objective. 
# MAGIC * Be sure to use an evaluation metric that aligns with your specified objective.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Remember that we assess our models against our test data set to ensure that our solutions generalize.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If your solution doesn't meet the objective, consider tweaking the model and data used by the model until it does meet the objective.

# COMMAND ----------

# MAGIC %md
# MAGIC After completing all of the above objectives, you should be ready to communicate your results. Move to the next video in the lesson for a description on that part of the project.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
