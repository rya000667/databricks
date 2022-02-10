# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Logistic Regression Lab 2
# MAGIC 
# MAGIC **Objectives**:
# MAGIC 1. Perform a train-test split on data.
# MAGIC 1. Evaluate four multi-variable logistic regression models using accuracy
# MAGIC and a confusion matrix.
# MAGIC 
# MAGIC Additionally, you will be asked to consider overfitting and underfitting
# MAGIC of the models based upon these results.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC ### Load the Data
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
# MAGIC ### Prepare Four Datasets and Target
# MAGIC 
# MAGIC Next, we will prepare four subsets of our, used as in the previous lab
# MAGIC to build four different logistic regression models.
# MAGIC 
# MAGIC We also prepare our target vector, `y`.

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

X_1 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_resting_heartrate']]
X_2 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_vo2']]
X_3 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2']]
X_4 = ht_agg_pandas_df[['mean_active_heartrate', 'mean_bmi', 'mean_vo2', 'mean_resting_heartrate']]

le = LabelEncoder()
lifestyle = ht_agg_pandas_df['lifestyle']
le.fit(lifestyle)
y = le.transform(lifestyle)

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
# MAGIC 
# MAGIC ### Exercise 1: Perform the Train-Test Split
# MAGIC 
# MAGIC Perform the train-test split on the remaining data subsets:
# MAGIC 1. use the helper function `train_test_split`
# MAGIC 1. split the following subsets:
# MAGIC    - `X_2`, `X_3`, `X_4`

# COMMAND ----------

# TODO
X_2_train, X_2_test, y_2_train, y_2_test = train_test_split(X_2, y)
X_3_train, X_3_test, y_3_train, y_3_test = train_test_split(X_3, y)
X_4_train, X_4_test, y_4_train, y_4_test = train_test_split(X_4, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Multi-Variable Logistic Regression
# MAGIC 
# MAGIC Fit four multiple-variable logistic models, one for each datasubset.

# COMMAND ----------

ht_agg_pandas_df.columns

# COMMAND ----------

y.shaepe()

# COMMAND ----------

# TODO
from sklearn.linear_model import LogisticRegression
lr_1 = LogisticRegression(max_iter=10000)
lr_2 = LogisticRegression(max_iter=10000)
lr_3 = LogisticRegression(max_iter=10000)
lr_4 = LogisticRegression(max_iter=10000)

# training for fit
lr_1.fit(X_1_train, y_train)
lr_2.fit(X_2_train, y_2_train)
lr_3.fit(X_3_train, y_3_train)
lr_4.fit(X_4_train, y_4_train)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC ### Evaluate a Multi-variable Model using accuracy and a confusion matrix
# MAGIC 
# MAGIC Finally, we evaulate our models. We do so using the accuracy metric and a confusion matrix.
# MAGIC 
# MAGIC To use these metrics, we need to
# MAGIC 1. generate a vector of precictions using `estimator.predict()`
# MAGIC 1. pass actual and predicted values to the metric as `metric(actual, predicted)`
# MAGIC 1. do this for both the training and testing data

# COMMAND ----------

from sklearn.metrics import accuracy_score, confusion_matrix

y_train_1_predicted = lr_1.predict(X_1_train)
y_test_1_predicted = lr_1.predict(X_1_test)

print("training accuracy: ", accuracy_score(y_train, y_train_1_predicted))
print("test accuracy:     ", accuracy_score(y_test, y_test_1_predicted))
print("training confusion matrix")
print(confusion_matrix(y_train, y_train_1_predicted))
print("")
print("test confusion matrix")
print(confusion_matrix(y_test, y_test_1_predicted))

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: What do you notice about the results?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Turn
# MAGIC ### Exercise 3: Generate Predictions
# MAGIC 1. use the following subset splits:
# MAGIC    - `X_1_test`, `X_2_test`, `X_3_test`, `X_4_test`
# MAGIC    - `X_1_train`, `X_2_train`, `X_3_train`, `X_4_train`

# COMMAND ----------

# TODO
y_train_1_predicted = lr_1.predict(X_1_train)
y_test_1_predicted = lr_1.predict(X_1_test)
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
# MAGIC 1. Use the `accuracy_score` and `confusion_matrix` metrics
# MAGIC 1. don't forget to take the square root of the mean squared error
# MAGIC 1. use the following subset splits:
# MAGIC    - `X_2_test`, `X_3_test`, `X_4_test`
# MAGIC    - `X_2_train`, `X_3_train`, `X_4_train`

# COMMAND ----------

# TODO
# accuracy_score(y_train, y_train_1_predicted))
'''
y_train_1_predicted = lr_1.predict(X_1_train)
y_test_1_predicted = lr_1.predict(X_1_test)
y_train_2_predicted = lr_2.predict(X_2_train)
y_test_2_predicted = lr_2.predict(X_2_test)
y_train_3_predicted = lr_3.predict(X_3_train)
y_test_3_predicted = lr_3.predict(X_3_test)
y_train_4_predicted = lr_4.predict(X_4_train)
y_test_4_predicted = lr_4.predict(X_4_test)
print(confusion_matrix(y_train, y_train_1_predicted))
'''
train_1_accuracy = accuracy_score(y_train, y_train_1_predicted)
train_1_conf_mat = confusion_matrix(y_train, y_train_1_predicted)
test_1_accuracy = accuracy_score(y_test, y_test_1_predicted)
test_1_conf_mat = confusion_matrix(y_test, y_test_1_predicted)

train_2_accuracy = accuracy_score(y_2_train, y_train_2_predicted)
train_2_conf_mat = confusion_matrix(y_2_train, y_train_2_predicted)
test_2_accuracy = accuracy_score(y_2_test, y_test_2_predicted)
test_2_conf_mat = confusion_matrix(y_2_test, y_test_2_predicted)

train_3_accuracy = accuracy_score(y_3_train, y_train_3_predicted)
train_3_conf_mat = confusion_matrix(y_3_train, y_train_3_predicted)
test_3_accuracy = accuracy_score(y_3_test, y_test_3_predicted)
test_3_conf_mat = confusion_matrix(y_4_test, y_test_4_predicted)

train_4_accuracy = accuracy_score(y_4_train, y_train_4_predicted)
train_4_conf_mat = confusion_matrix(y_4_train, y_train_4_predicted)
test_4_accuracy = accuracy_score(y_4_test, y_test_4_predicted)
test_4_conf_mat = confusion_matrix(y_4_test, y_test_4_predicted)

print("model 1: training accuracy: ", train_1_accuracy)
print("model 1: training confusion matrix: ")
print(train_1_conf_mat)
print(" ")
print("model 1: test accuracy:     ", test_1_accuracy)
print("model 1: test confusion matrix:     ")
print(test_1_conf_mat)
print(" ")
print("model 2: training accuracy: ", train_2_accuracy)
print("model 2: training confusion matrix: ")
print(train_2_conf_mat)
print(" ")
print("model 2: test accuracy:     ", test_2_accuracy)
print("model 2: test confusion matrix:     ")
print(test_2_conf_mat)
print(" ")
print("model 3: training accuracy: ", train_3_accuracy)
print("model 3: training confusion matrix: ")
print(train_3_conf_mat)
print(" ")
print("model 3: test accuracy:     ", test_3_accuracy)
print("model 3: test confusion matrix:     ")
print(test_3_conf_mat)
print(" ")
print("model 4: training accuracy: ", train_4_accuracy)
print("model 4: training confusion matrix: ")
print(train_4_conf_mat)
print(" ")
print("model 4: test accuracy:     ", test_4_accuracy)
print("model 4: test confusion matrix:     ")
print(test_4_conf_mat)
print(" ")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Question**: Which of these models is the best at predicting lifestyle?
# MAGIC 
# MAGIC **Question**: Do any of the models show signs of overfitting?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
