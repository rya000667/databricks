# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Model Generalization
# MAGIC 
# MAGIC **Objectives**:
# MAGIC 1. Perform a train-test split on data.
# MAGIC 1. Evaluate four multi-variable linear regression models using RMSE and MAE.
# MAGIC 
# MAGIC Additionally, you will be asked to consider overfitting and underfitting
# MAGIC of the models based upon these results.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This lab is meant to build on your work from the previous lab. Some of the steps are identical and have been marked with the note: **REVIEW**. Please run the corresponding code for these steps and feel free to look them over as a review of your previous lab work. 

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
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
# MAGIC Next, we will prepare four subsets of our, used as in the previous lab
# MAGIC to build four different linear models.
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
# MAGIC ### Perform the Train-Test Split
# MAGIC 
# MAGIC Next, we will split one of our four subsets of feature data and our target data
# MAGIC into training and testing data.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_1_train, X_1_test, y_train, y_test = train_test_split(X_1, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC ### Exercise 1: Perform the Train-Test Split
# MAGIC 
# MAGIC Perform the train-test split on the remaining data subsets:
# MAGIC 1. use the helper function `train_test_split`
# MAGIC 1. split the following subsets:
# MAGIC    - `X_2`, `X_3`, `X_4`

# COMMAND ----------

# ANSWER
X_2_train, X_2_test, y_train, y_test = train_test_split(X_2, y)
X_3_train, X_3_test, y_train, y_test = train_test_split(X_3, y)
X_4_train, X_4_test, y_train, y_test = train_test_split(X_4, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Multi-Variable Linear Regression
# MAGIC 
# MAGIC Fit four multiple-variable linear models, one for each datasubset.

# COMMAND ----------

# ANSWER
from sklearn.linear_model import LinearRegression
lr_1 = LinearRegression()
lr_2 = LinearRegression()
lr_3 = LinearRegression()
lr_4 = LinearRegression()

lr_1.fit(X_1_train, y_train)
lr_2.fit(X_2_train, y_train)
lr_3.fit(X_3_train, y_train)
lr_4.fit(X_4_train, y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC ### Evaluate a Multi-variable Model using RMSE and MAE
# MAGIC 
# MAGIC Finally, we evaulate the training and test models. We do so using the RMSE and MAE
# MAGIC metrics.
# MAGIC 
# MAGIC To use these metrics, we need to
# MAGIC 1. generate a vector of precictions using `estimator.predict()`
# MAGIC 1. pass actual and predicted values to the metric as `metric(actual, predicted)`
# MAGIC 1. do this for both the training and testing data

# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error

y_train_1_predicted = lr_1.predict(X_1_train)
y_test_1_predicted = lr_1.predict(X_1_test)

print("training mse: ", mean_squared_error(y_train, y_train_1_predicted))
print("test mse:     ", mean_squared_error(y_test, y_test_1_predicted))
print("training mae: ", mean_absolute_error(y_train, y_train_1_predicted))
print("test mae:     ", mean_absolute_error(y_test, y_test_1_predicted))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MSE vs. RMSE
# MAGIC 
# MAGIC Note that our metrics, mse and mae are on different scales.
# MAGIC Let's take the square root of the mse to put them on the same scale.

# COMMAND ----------

import numpy as np
train_1_rmse = np.sqrt(mean_squared_error(y_train, y_train_1_predicted))
test_1_rmse = np.sqrt(mean_squared_error(y_test, y_test_1_predicted))
train_1_mae = mean_absolute_error(y_train, y_train_1_predicted)
test_1_mae = mean_absolute_error(y_test, y_test_1_predicted)

print("model 1: training rmse: ", train_1_rmse)
print("model 1: training mae: ", train_1_mae)
print("model 1: test rmse:     ", test_1_rmse)
print("model 1: test mae:     ", test_1_mae)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC ### Exercise 3: Generate Predictions
# MAGIC 1. use the following subset splits:
# MAGIC    - `X_2_test`, `X_3_test`, `X_4_test`
# MAGIC    - `X_2_train`, `X_3_train`, `X_4_train`

# COMMAND ----------

# ANSWER
y_train_2_predicted = lr_2.predict(X_2_train)
y_test_2_predicted = lr_2.predict(X_2_test)
y_train_3_predicted = lr_3.predict(X_3_train)
y_test_3_predicted = lr_3.predict(X_3_test)
y_train_4_predicted = lr_4.predict(X_4_train)
y_test_4_predicted = lr_4.predict(X_4_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4: Evaluate Our Models
# MAGIC 
# MAGIC 1. Use the `mean_squared_error` and `mean_absolute_error` metrics
# MAGIC 1. don't forget to take the square root of the mean squared error
# MAGIC 1. use the following subset splits:
# MAGIC    - `X_2_test`, `X_3_test`, `X_4_test`
# MAGIC    - `X_2_train`, `X_3_train`, `X_4_train`

# COMMAND ----------

# ANSWER
train_2_rmse = np.sqrt(mean_squared_error(y_train, y_train_2_predicted))
train_2_mae = mean_absolute_error(y_train, y_train_2_predicted)
test_2_rmse = np.sqrt(mean_squared_error(y_test, y_test_2_predicted))
test_2_mae = mean_absolute_error(y_test, y_test_2_predicted)

train_3_rmse = np.sqrt(mean_squared_error(y_train, y_train_3_predicted))
train_3_mae = mean_absolute_error(y_train, y_train_3_predicted)
test_3_rmse = np.sqrt(mean_squared_error(y_test, y_test_3_predicted))
test_3_mae = mean_absolute_error(y_test, y_test_3_predicted)

train_4_rmse = np.sqrt(mean_squared_error(y_train, y_train_4_predicted))
train_4_mae = mean_absolute_error(y_train, y_train_4_predicted)
test_4_rmse = np.sqrt(mean_squared_error(y_test, y_test_4_predicted))
test_4_mae = mean_absolute_error(y_test, y_test_4_predicted)

print("model 1: training rmse: ", train_1_rmse)
print("model 1: training mae: ", train_1_mae)
print("model 1: test rmse:     ", test_1_rmse)
print("model 1: test mae:     ", test_1_mae)
print("model 2: training rmse: ", train_2_rmse)
print("model 2: training mae: ", train_2_mae)
print("model 2: test rmse:     ", test_2_rmse)
print("model 2: test mae:     ", test_2_mae)
print("model 3: training rmse: ", train_3_rmse)
print("model 3: training mae: ", train_3_mae)
print("model 3: test rmse:     ", test_3_rmse)
print("model 3: test mae:     ", test_3_mae)
print("model 4: training rmse: ", train_4_rmse)
print("model 4: training mae: ", train_4_mae)
print("model 4: test rmse:     ", test_4_rmse)
print("model 4: test mae:     ", test_4_mae)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Question**:: Which of these models is the best at predicting mean steps?
# MAGIC 
# MAGIC **Question**: Do any of the models show signs of overfitting?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
