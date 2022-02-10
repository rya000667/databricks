# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Linear Regression Lab 2
# MAGIC 
# MAGIC **Objectives**:
# MAGIC 1. Evaluate four multi-variable linear regression models using RMSE and MAE.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This lab is meant to build on your work from the previous lab. Some of the steps are identical and have been marked with the note: **REVIEW**. Please run the corresponding code for these steps and feel free to look them over as a review of your previous lab work. 

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC ### Load the Data
# MAGIC 
# MAGIC **REVIEW** 
# MAGIC 
# MAGIC The `Includes/Classroom-Setup` notebook has made an aggregate table of data
# MAGIC available to us via the Metastore associated with our workspace. We can load
# MAGIC the data as a pandas dataframe using the cell below.
# MAGIC 
# MAGIC This command loads the table using the Metastore reference. The `.toPandas()`
# MAGIC method converts the Spark DataFrame to a Pandas DataFrame. We will use the
# MAGIC Pandas DataFrame with Scikit-Learn throughout this Module.

# COMMAND ----------

ht_agg_spark_df = spark.read.table("ht_agg")
ht_agg_pandas_df = ht_agg_spark_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Four Datasets
# MAGIC 
# MAGIC **REVIEW**
# MAGIC 
# MAGIC Next, we will prepare four subsets of our data which we will use to build four different linear models.
# MAGIC 
# MAGIC We also prepare our target vector, `y`.

# COMMAND ----------

X_1 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_resting_heartrate']]
X_2 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_vo2']]
X_3 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2']]
X_4 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2', 'mean_resting_heartrate']]
y = ht_agg_pandas_df['mean_steps']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Framing a Business Problem
# MAGIC 
# MAGIC **REVIEW**
# MAGIC 
# MAGIC We have spoken frequently about the entire data science process starting
# MAGIC with a good question. Over the next few labs, we will use supervised machine learning
# MAGIC to answer the following business question:
# MAGIC 
# MAGIC > Given a users fitness profile, can we predict the average number of steps they
# MAGIC are likely to take each day?
# MAGIC 
# MAGIC Here, our **inputs** will be fitness profile information and our **output**
# MAGIC will be the average number of daily steps. The fitness profile information
# MAGIC consists of average daily measurements of BMI, VO2, and resting and active heartrates.
# MAGIC 
# MAGIC We will perform supervised learning to develop a function to map these inputs to average
# MAGIC daily steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC ### Multi-Variable Linear Regression
# MAGIC 
# MAGIC **REVIEW**
# MAGIC 
# MAGIC Fit four multiple-variable linear models, one for each datasubset.

# COMMAND ----------

from sklearn.linear_model import LinearRegression
lr_1 = LinearRegression()
lr_2 = LinearRegression()
lr_3 = LinearRegression()
lr_4 = LinearRegression()

lr_1.fit(X_1, y)
lr_2.fit(X_2, y)
lr_3.fit(X_3, y)
lr_4.fit(X_4, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate a Multi-variable Model using RMSE and MAE
# MAGIC 
# MAGIC Finally, we evaulate our models. We do so using the RMSE and MAE
# MAGIC metrics.
# MAGIC 
# MAGIC To use these metrics, we need to
# MAGIC 1. generate a vector of precictions using `estimator.predict()`
# MAGIC 1. pass actual and predicted values to the metric as `metric(actual, predicted)`
# MAGIC 1. do this for both the ing and testing data

# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error

y_1_predicted = lr_1.predict(X_1)

print("mse: ", mean_squared_error(y, y_1_predicted))
print("mae: ", mean_absolute_error(y, y_1_predicted))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MSE vs. RMSE
# MAGIC 
# MAGIC Note that our metrics, mse and mae are on different scales.
# MAGIC Let's take the square root of the mse to put them on the same scale.

# COMMAND ----------

import numpy as np
rmse_1 = np.sqrt(mean_squared_error(y, y_1_predicted))
mae_1 = mean_absolute_error(y, y_1_predicted)

print("model 1: rmse: ", rmse_1)
print("model 1: mae: ", mae_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC ### Exercise 1: Generate Predictions
# MAGIC 
# MAGIC Perform the train-test split on the remaining data subsets:
# MAGIC 1. use the following subsets:
# MAGIC    - `X_2`, `X_3`, `X_4`

# COMMAND ----------

# ANSWER
y_2_predicted = lr_2.predict(X_2)
y_3_predicted = lr_3.predict(X_3)
y_4_predicted = lr_4.predict(X_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Evaluate Our Models
# MAGIC 
# MAGIC 1. Use the `mean_squared_error` and `mean_absolute_error` metrics
# MAGIC 1. don't forget to take the square root of the mean squared error
# MAGIC 1. use the following subset splits:
# MAGIC    - `X_2`, `X_3`, `X_4`

# COMMAND ----------

# ANSWER
rmse_2 = np.sqrt(mean_squared_error(y, y_2_predicted))
mae_2 = mean_absolute_error(y, y_2_predicted)
rmse_3 = np.sqrt(mean_squared_error(y, y_3_predicted))
mae_3 = mean_absolute_error(y, y_3_predicted)
rmse_4 = np.sqrt(mean_squared_error(y, y_4_predicted))
mae_4 = mean_absolute_error(y, y_4_predicted)

print("model 1: rmse: ", rmse_1)
print("model 1: mae: ", mae_1)
print("model 2: rmse: ", rmse_2)
print("model 2: mae: ", mae_2)
print("model 3: rmse: ", rmse_3)
print("model 3: mae: ", mae_3)
print("model 4: rmse: ", rmse_4)
print("model 4: mae: ", mae_4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: Which of these models is best at predicting mean steps?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
