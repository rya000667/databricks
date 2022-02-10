# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Discrete Probability
# MAGIC 
# MAGIC **Objective**: *Use the basic rules of discrete probability to complete a series of exercises.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises about the basic rules of discrete probability.
# MAGIC 
# MAGIC Use the following rules to complete the exercises below:
# MAGIC - Additive rule of probability: P(A or B) = P(A) + P(B) - P(A and B)
# MAGIC - Multiplicative rule of probability: P(A and B) = P(A) * P(B)
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Python can be used as a calculator. Please use Python to complete the following exercises, like below.

# COMMAND ----------

# MAGIC %python
# MAGIC 1/4 + 1/4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Compute the probability of rolling a 2, 4, or 5 when rolling a single, six-sided die.

# COMMAND ----------

# MAGIC %python
# MAGIC # ANSWER
# MAGIC 1/6 + 1/6 + 1/6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Compute the probability of rolling a 2 and then a 6 when rolling a pair of dice.

# COMMAND ----------

# MAGIC %python
# MAGIC # ANSWER
# MAGIC (1/6) * (1/6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Compute the probability of randomly selecting a Queen or King from a standard deck of cards.

# COMMAND ----------

# MAGIC %python
# MAGIC # ANSWER
# MAGIC (1/13) * (1/13)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Compute the probability of flipping a coin twice with it landing on heads the first time and tails the second time.

# COMMAND ----------

# MAGIC %python
# MAGIC # ANSWER
# MAGIC (1/2) * (1/2)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
