# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Bernoulli Distribution (20 mins)
# MAGIC 
# MAGIC **Objective**: *Use a Bernoulli distribution to model a real-world scenario.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises where you model a real-world scenario using the Bernoulli distribution.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1
# MAGIC Random variable X represents whether we roll a number divisible by 3 on a six sided die.
# MAGIC - Let x = 1 represent success (rolling a 3 or 6)
# MAGIC - Let x = 0 represent failure (rolling a 1, 2, 4, or 5)
# MAGIC 
# MAGIC The probability of an outcome n can be computed using the probability density function of a Bernoulli distribution:
# MAGIC 
# MAGIC P(x) = p<sup>x</sup>(1-p)<sup>1-x</sup>, where p is the probability of success.
# MAGIC 
# MAGIC Compute the probability of success (x=1) using this function.

# COMMAND ----------

# TODO
p = 1/3
x = 1
prob_success = p**x * (1-p)**(1-x)
print(f"The probability of success is {prob_success}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Use the same function to compute the probability of failure (x=0).

# COMMAND ----------

# TODO
p = 1/3
x = 1
prob_success = p**x * (1-p)**(1-x)
prob_failure = 1-prob_success
print(f"The probability of failure is {prob_failure}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Get the expected value and variance of this distribution.
# MAGIC - expected value = p
# MAGIC - variance = p(1-p)

# COMMAND ----------

# TODO
p = <FILL_IN>
expected_value = <FILL_IN>
variance = <FILL_IN>

print(f"The expected value is {expected value} and the variance is {variance}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC So far, we've been working with probabilities using Python as a calculator. However, we can also work with probabilities using the `scipy.stats` module.
# MAGIC 
# MAGIC Complete the below code block to compute the expected value (mean) and varience using `scipy.stats`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can use the [documentation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bernoulli.html) as a reference.

# COMMAND ----------

from scipy.stats import bernoulli

help(bernoulli.stats)

# COMMAND ----------

# TODO
from scipy.stats import bernoulli
expected_value, variance, skew = bernoulli.stats(p, moments="mvs")
print(f"The expected value is {expected_value} and the variance is {variance} and the skew is {skew}")

# COMMAND ----------

from scipy.stats import bernoulli
expected_value, variance = bernoulli.stats(p, moments="mv")
print(f"The expected value is {expected_value} and the variance is {variance}")

# COMMAND ----------

# MAGIC %md
# MAGIC Congratualations on completing the Bernoulli Distribution lab!
# MAGIC 
# MAGIC Next, we will continue by learning about continuous probability. Learning to use Python modules like `scipy.stats` will be incredibly helpful when working with continuous probability.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
