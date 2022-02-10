# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Linear Regression Lab 1
# MAGIC 
# MAGIC **Objectives**:
# MAGIC 1. Develop a single-variable linear regression model.
# MAGIC 2. Develop a multi-variable linear regression model.
# MAGIC 
# MAGIC In this lab, we'll do a quick demonstration of single-variable and multi-variable linear regression using Python and Scikit-Learn.
# MAGIC 
# MAGIC After each demonstration, you'll have the opportunity to complete the exercises yourself.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/dsfda.db/ht_agg', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC ### Load the Data
# MAGIC The `../../Includes/Classroom-Setup` notebook has made an aggregate table of data available to us. 
# MAGIC 
# MAGIC We can load the data as a Pandas DataFrame using the cell below. The `.toPandas()` method converts the Spark DataFrame to a Pandas DataFrame. We will use the Pandas DataFrame with Scikit-Learn throughout this Module.

# COMMAND ----------

ht_agg_spark_df = spark.read.table("ht_agg")
ht_agg_pandas_df = ht_agg_spark_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Framing a Business Problem
# MAGIC 
# MAGIC We have spoken frequently about the entire data science process starting with a good question. 
# MAGIC 
# MAGIC Over the next few labs, we will use supervised machine learning to answer the following business question:
# MAGIC 
# MAGIC > Given a users fitness profile, can we predict the average number of steps they are likely to take each day?
# MAGIC 
# MAGIC Here, our **inputs** will be fitness profile information and our **output** will be the average number of daily steps. The fitness profile information consists of average daily measurements of BMI, VO2, and resting and active heartrates.
# MAGIC 
# MAGIC We will perform supervised learning to develop a function to map these inputs to average daily steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scikit-Learn
# MAGIC 
# MAGIC #### Overview
# MAGIC 
# MAGIC One of the most popular libraries for doing machine learning in Python.
# MAGIC 
# MAGIC Scikit-Learn features:
# MAGIC - Simple and efficient tools for predictive data analysis
# MAGIC - Accessible to everybody, and reusable in various contexts
# MAGIC - Built on NumPy, SciPy, and matplotlib
# MAGIC - Open source, commercially usable - BSD license
# MAGIC 
# MAGIC #### The Scikit-Learn `estimator` API
# MAGIC 
# MAGIC The main API for performing machine learning with sklearn is the **estimator** API.
# MAGIC 
# MAGIC An estimator is any object that learns from data; it may be a
# MAGIC 
# MAGIC - A **predictor**
# MAGIC    - classification algorithm
# MAGIC    - regression algorithm
# MAGIC    - clustering algorithm
# MAGIC - A **transformer** that extracts/filters useful features from raw data
# MAGIC 
# MAGIC #### Fitting a predictor model with sklearn
# MAGIC 
# MAGIC - All estimator objects expose a `.fit()` method.
# MAGIC - For supervised learning, this looks like `predictor.fit(features, target)`
# MAGIC 
# MAGIC ```
# MAGIC estimator.fit(data)
# MAGIC ```
# MAGIC 
# MAGIC #### Evaluating a model with sklearn
# MAGIC 
# MAGIC - All predictor objects expose a `.score()` method
# MAGIC - For supervised learning, this looks like `predictor.score(features, target)`
# MAGIC - sklearn provides a built-in metric depending upon whether a classification or regression algorithm is being used
# MAGIC - For classification, `predictor.score(features, target)`, uses the accuracy metric
# MAGIC - For regression, `predictor.score(features, target)`, uses the R2 metric

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC 
# MAGIC ### Single-Variable Linear Regression
# MAGIC 
# MAGIC Our first set of models will have a single independent variable (or single feature) and a single dependent variable (or single target).
# MAGIC 
# MAGIC A way to think about the relationship between feature and target is to put them both into a sentence, "for a [feature] of [value], we would predict that this user would have [value] [target]".
# MAGIC 
# MAGIC In our case , we might have an assumption that the feature `mean_bmi` is predictive of our target `mean_steps`, so our sentence could read:
# MAGIC 
# MAGIC > "For a mean BMI of 20, we would predict that this user would have 4000 mean steps."
# MAGIC 
# MAGIC 
# MAGIC Our intution and domain knowledge can help us discern predictive features.
# MAGIC 
# MAGIC ### Setting up Linear Regression
# MAGIC 
# MAGIC First, we'll import our estimator of choice, a predictor called Linear Regression.

# COMMAND ----------

from sklearn.linear_model import LinearRegression

# COMMAND ----------

# MAGIC %md
# MAGIC Then, we'll instantiate or create an instance of our estimator.

# COMMAND ----------

lr = LinearRegression()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Feature Vectors
# MAGIC 
# MAGIC 🧐 sklearn wants the shape of our data to be a matrix for our feature(s) and the shape of our target to be a vector. This is why you will see two square brackets around our feature - a matrix - and a single set of square brackets around our target - a vector.

# COMMAND ----------

X = ht_agg_pandas_df[['mean_bmi']]
y = ht_agg_pandas_df['mean_steps']

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

# COMMAND ----------

lr.score(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC 
# MAGIC ### Exercise 1: Single-Variable Linear Regression
# MAGIC 
# MAGIC Fit a single-variable linear model for each of the remaining feature.
# MAGIC 1. prepare a feature matrix for each of these features:
# MAGIC  - `mean_bmi`
# MAGIC  - `mean_active_heartrate`
# MAGIC  - `mean_resting_heartrate`
# MAGIC  - `mean_vo2`
# MAGIC 1. fit a single-variable linear model for each of these features
# MAGIC 1. evaluate using `.score()` each of these models and print the result

# COMMAND ----------

ht_agg_pandas_df

# COMMAND ----------

# TODO
X_bmi = ht_agg_pandas_df[['mean_bmi']]
X_active_heartrate = ht_agg_pandas_df[['mean_active_heartrate']]
X_resting_heartrate = ht_agg_pandas_df[['mean_resting_heartrate']]
X_vo2 = ht_agg_pandas_df[['mean_vo2']]

lr_bmi = LinearRegression()
lr_active_heartrate = LinearRegression()
lr_resting_heartrate = LinearRegression()
lr_vo2 = LinearRegression()

# fit(feature,target)
lr_bmi.fit(X_bmi, y)
lr_active_heartrate.fit(X_active_heartrate, y)
lr_resting_heartrate.fit(X_resting_heartrate, y)
lr_vo2.fit(X_vo2, y)


print(f"bmi:  {lr_bmi.score(X_bmi, y)}")
print(f"active_heartrate: {lr_active_heartrate.score(X_active_heartrate,y)} " )
print(f"resting_heartrate: {lr_resting_heartrate.score(X_resting_heartrate, y)}")
print(f"vo2: {lr_vo2.score(X_vo2, y)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC ### Multiple-Variable Linear Regression
# MAGIC 
# MAGIC Our next set of models will use more that one feature and but still have
# MAGIC a single target.
# MAGIC 
# MAGIC We can apply similar logic in forming a sentence to describe the relationship "for a [feature1] of [value1] and a [feature2] of [value2], we would predict that this user would have [value] [target]".
# MAGIC 
# MAGIC e.g.
# MAGIC > "For a mean BMI of 20 and a mean active heartrate of 125, we would predict that this user would have 9500 mean steps."
# MAGIC 
# MAGIC Let's try this model out.

# COMMAND ----------

ht_agg_pandas_df['mean_active_heartrate']

# COMMAND ----------

ht_agg_pandas_df['mean_active_heartrate'].sample()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display results from previous models
# MAGIC 
# MAGIC Before we train this new model, let's display the results from the previous models
# MAGIC for comparison.

# COMMAND ----------

print("bmi:               ", lr_bmi.score(X_bmi, y))
print("active_heartrate:  ", lr_active_heartrate.score(X_active_heartrate, y))
print("resting_heartrate: ", lr_resting_heartrate.score(X_resting_heartrate, y))
print("vo2:               ", lr_vo2.score(X_vo2, y))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train new multiple-variable linear regression
# MAGIC 
# MAGIC Train the new model using both `mean_bmi` and `mean_active_heartrate` as predictors.

# COMMAND ----------

X_bmi_act_hr = ht_agg_pandas_df[['mean_bmi', 'mean_active_heartrate']]
lr_bmi_act_hr = LinearRegression()
lr_bmi_act_hr.fit(X_bmi_act_hr, y)
print("bmi_act_hr: ", lr_bmi_act_hr.score(X_bmi_act_hr, y))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Your Turn
# MAGIC ### Exercise 2: Multi-Variable Linear Regression
# MAGIC 😎 Note that this two feature model performs better than any of the single feature models.
# MAGIC 
# MAGIC Fit four multiple-variable linear models.
# MAGIC 1. prepare a feature matrix
# MAGIC 1. fit a linear model for each of feature matrix
# MAGIC 1. evaluate each model using `.score()` and print the result
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Did you try any models with more than two features? Multiple-variable
# MAGIC linear models can use any or all of the features.

# COMMAND ----------

# TODO
X_1 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_resting_heartrate']]
X_2 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_vo2']]
X_3 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2']]
X_4 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2', 'mean_resting_heartrate']]

lr_1 = LinearRegression()
lr_2 = LinearRegression()
lr_3 = LinearRegression()
lr_4 = LinearRegression()

lr_1.fit(X_1, y)
lr_2.fit(X_2, y)
lr_3.fit(X_3, y)
lr_4.fit(X_4, y)

print("model 1: ", lr_1.score(X_1, y))
print("model 2: ", lr_2.score(X_2, y))
print("model 3: ", lr_3.score(X_3, y))
print("model 4: ", lr_4.score(X_4, y))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
