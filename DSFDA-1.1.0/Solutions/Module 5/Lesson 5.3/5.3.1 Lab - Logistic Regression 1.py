# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Logistic Regression Lab 1
# MAGIC 
# MAGIC **Objectives**:
# MAGIC 1. Develop a single-variable logistic regression model.
# MAGIC 1. Develop a multi-variable logistic regression model.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC ### Load the Data
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
# MAGIC ### Framing a Business Problem
# MAGIC 
# MAGIC Over the next few labs, we will use supervised machine learning
# MAGIC to answer a new business question:
# MAGIC 
# MAGIC > Given a users fitness profile, can we predict the lifestyle of a user?
# MAGIC 
# MAGIC Like the regression problem we previously solved,
# MAGIC our **inputs** will be fitness profile information. This is, however, a classification
# MAGIC problem and will have a different **output**, lifestyle.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Scikit-Learn `estimator` API
# MAGIC 
# MAGIC Once more, we will use the sklearn **estimator** API.
# MAGIC 
# MAGIC The good news is that we use the exact same pattern for classification as we did
# MAGIC for regression.
# MAGIC 
# MAGIC ```
# MAGIC estimator.fit(features, target)
# MAGIC estimator.score(features, target)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC 
# MAGIC ### Single-Variable Logistic Regression
# MAGIC 
# MAGIC 
# MAGIC First, we'll import our estimator of choice, a predictor called Logistic Regression.

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

# COMMAND ----------

# MAGIC %md
# MAGIC Then, we'll instantiate or create an instance of our estimator.

# COMMAND ----------

lr = LogisticRegression(max_iter=10000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Feature Vectors
# MAGIC 
# MAGIC 🧐 sklearn wants the shape of our data to be a matrix for our feature(s)
# MAGIC and the shape of our target to be a vector. This is why you will see two square
# MAGIC brackets around our feature - a matrix - and a single set of square brackets
# MAGIC around our target - a vector.

# COMMAND ----------

X = ht_agg_pandas_df[['mean_bmi']]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Target Vector
# MAGIC 
# MAGIC An additional step, not required when perform the linear regression,
# MAGIC is necessary to encode our target vector when performing a logistic regression.
# MAGIC 
# MAGIC This has to do with the way the lifestyle lables are stored.

# COMMAND ----------

ht_agg_pandas_df["lifestyle"].unique()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Each lifestyle is recorded as a string value.
# MAGIC 
# MAGIC sklearn models can only work on numerical values. For this reason,
# MAGIC it is required to numerically encode our lifestyle values.
# MAGIC 
# MAGIC We will use an sklearn transformer to do this encoding.
# MAGIC 
# MAGIC An sklearn transformer is like an sklearn estimator except rather than
# MAGIC using it to `.predict()` or `.score()`, we will use it to `.transform()`
# MAGIC 
# MAGIC ```
# MAGIC estimator.fit(data)
# MAGIC estimator.transform(data)
# MAGIC ```

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

le = LabelEncoder()
lifestyle = ht_agg_pandas_df['lifestyle']
le.fit(lifestyle)
y = le.transform(lifestyle)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fit the Model
# MAGIC 
# MAGIC Next, fit our model, using the same `.fit(feature, target)` pattern we learned earlier.
# MAGIC 
# MAGIC The model will learn the relationship between features and target, i.e.
# MAGIC we will "train or fit the model".

# COMMAND ----------

lr.fit(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate the model
# MAGIC 
# MAGIC Finally, use the `.score()` method to evaluate the single-variable model.
# MAGIC 
# MAGIC Note that a classifier estimator in sklearn uses accuracy for scoring
# MAGIC by default.

# COMMAND ----------

lr.score(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC 
# MAGIC ### Exercise 1: Single-Variable Logistic Regression
# MAGIC 
# MAGIC Fit a single-variable logistic model for each of the remaining feature.
# MAGIC 1. prepare a feature matrix for each of these features:
# MAGIC  - `mean_bmi`
# MAGIC  - `mean_active_heartrate`
# MAGIC  - `mean_resting_heartrate`
# MAGIC  - `mean_vo2`
# MAGIC 1. fit a single-variable logistic model for each of these features
# MAGIC 1. evaluate using `.score()` each of these models and print the result

# COMMAND ----------

# ANSWER
X_bmi = ht_agg_pandas_df[['mean_bmi']]
X_active_heartrate = ht_agg_pandas_df[['mean_active_heartrate']]
X_resting_heartrate = ht_agg_pandas_df[['mean_resting_heartrate']]
X_vo2 = ht_agg_pandas_df[['mean_vo2']]

lr_bmi = LogisticRegression(max_iter=10000)
lr_active_heartrate = LogisticRegression(max_iter=10000)
lr_resting_heartrate = LogisticRegression(max_iter=10000)
lr_vo2 = LogisticRegression(max_iter=10000)

lr_bmi.fit(X_bmi, y)
lr_active_heartrate.fit(X_active_heartrate, y)
lr_resting_heartrate.fit(X_resting_heartrate, y)
lr_vo2.fit(X_vo2, y)

print("bmi:               ", lr_bmi.score(X_bmi, y))
print("active_heartrate:  ", lr_active_heartrate.score(X_active_heartrate, y))
print("resting_heartrate: ", lr_resting_heartrate.score(X_resting_heartrate, y))
print("vo2:               ", lr_vo2.score(X_vo2, y))

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: Which of these single-variable models is the best at predicting lifestyle?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC ### Multiple-Variable Logistic Regression
# MAGIC 
# MAGIC Our next set of models will use more that one feature and but still have
# MAGIC a single target.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display results from previous models
# MAGIC 
# MAGIC Before we train this new model, let's display the results from the previous models
# MAGIC for comparison.

# COMMAND ----------

print("bmi:               ", lr_bmi.score(X_bmi, y))
print("active_heartrate:  ", lr_active_heartrate.score(X_active_heartrate, y))
print("resting_heartrate: ", lr_resting_heartrate.score(X_resting_heartrate, y))
print("vo2:               ", lr_vo2.score(X_vo2, y))

# COMMAND ----------

X_bmi_act_hr = ht_agg_pandas_df[['mean_bmi', 'mean_active_heartrate']]
lr_bmi_act_hr = LogisticRegression(max_iter=10000)
lr_bmi_act_hr.fit(X_bmi_act_hr, y)
print("bmi_act_hr: ", lr_bmi_act_hr.score(X_bmi_act_hr, y))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC 
# MAGIC ### Exercise 2: Multi-Variable Logistic Regression
# MAGIC 😎 Note that this two feature model performs better than any of the single feature models.
# MAGIC 
# MAGIC Fit four multiple-variable logistic models.
# MAGIC 1. prepare a feature matrix
# MAGIC 1. fit a logistic model for each of feature matrix
# MAGIC 1. evaluate each model using `.score()` and print the result
# MAGIC 
# MAGIC 👨🏼‍🎤 Did you try any models with more than two features? Multiple-variable
# MAGIC logistic regression models can use any or all of the features.

# COMMAND ----------

# ANSWER
X_1 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_resting_heartrate']]
X_2 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_vo2']]
X_3 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2']]
X_4 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2', 'mean_resting_heartrate']]

lr_1 = LogisticRegression(max_iter=10000)
lr_2 = LogisticRegression(max_iter=10000)
lr_3 = LogisticRegression(max_iter=10000)
lr_4 = LogisticRegression(max_iter=10000)

lr_1.fit(X_1, y)
lr_2.fit(X_2, y)
lr_3.fit(X_3, y)
lr_4.fit(X_4, y)

print("model 1: ", lr_1.score(X_1, y))
print("model 2: ", lr_2.score(X_2, y))
print("model 3: ", lr_3.score(X_3, y))
print("model 4: ", lr_4.score(X_4, y))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which of these models is the best at predicting lifestyle?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
