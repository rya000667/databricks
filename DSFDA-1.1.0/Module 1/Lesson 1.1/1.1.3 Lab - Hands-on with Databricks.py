# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Hands-on with Databricks
# MAGIC 
# MAGIC **Objective**: *Familiarize yourself with the Databricks platform, the use of notebooks, and basic SQL operations in Databricks.*
# MAGIC 
# MAGIC In this lab, you will complete a series of exercises to familiarize yourself with the content covered in Lesson 0.1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC In order to execute code with Databricks, you need to have your notebook attached to an active cluster. 
# MAGIC 
# MAGIC Ensure that:
# MAGIC 1. You have created a cluster following the walkthrough of the video in this lesson.
# MAGIC 2. Your cluster's Databricks Runtime Version is 7.2 ML.
# MAGIC 3. Your cluster is active and running.
# MAGIC 4. This notebook is attached to your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC The fundamental piece of a Databricks notebook is the command cell. We use command cells to write and run our code. 
# MAGIC 
# MAGIC Complete the following:
# MAGIC 1. Insert a command cell beneath this one.
# MAGIC 2. Write `1 + 1` in the command cell.
# MAGIC 3. Run the command cell.
# MAGIC 4. Verify that the output of the executed code is `2`.

# COMMAND ----------

print(1+1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Command cells can also be used to add comments using a lightweight markup language named *markdown*. (That's how these command cells are written).
# MAGIC 
# MAGIC Complete the following:
# MAGIC 
# MAGIC 1. Double-click on this command cell.
# MAGIC 2. Notice the *magic command* at the top of the command cell that enables the use of markdown.
# MAGIC 3. Insert a command cell beneath this one and add the magic command to the first line.
# MAGIC 4. Write `THE MAGIC COMMAND FOR MARKDOWN IS _____` with the magic command filling the blank.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC THE MAGIC COMMAND FOR MARKDOWN IS %md

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Throughout this course, we will be using a setup file in each of our notebooks that connects Databricks to our data.
# MAGIC 
# MAGIC Complete the following:
# MAGIC 
# MAGIC 1. Run the below command cell to execute the setup file.
# MAGIC 2. Insert a SQL command cell beneath the command cell containg the setup file.
# MAGIC 3. Query all of the data in the table **`dsfda.ht_daily_metrics`** using the query `SELECT * FROM dsfda.ht_daily_metrics`.
# MAGIC 4. Examine the displayed table to learn about its columns and rows.

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dsfda.ht_daily_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 5
# MAGIC 
# MAGIC Throughout this course, we will need to manipulate data and save it as new tables using Delta, just as we did in the video during the lesson.
# MAGIC 
# MAGIC Complete the following:
# MAGIC 
# MAGIC 1. Insert a new SQL command cell beneath this one.
# MAGIC 2. Write a SQL query to return rows from the **dsfda.ht_users** table where the individual's lifestyle is `"Sedentary"`.
# MAGIC 3. Use the SQL query to create a new Delta table named **dsfda.ht_users_sedentary** and store the data in the following location: `"/dsfda/ht-users-sedentary"`.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   COUNT(*) AS totalSedentaryUsers
# MAGIC FROM dsfda.ht_users
# MAGIC 
# MAGIC WHERE
# MAGIC lifestyle = 'Sedentary'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Great job! You've completed the first lesson of the Data Science Fundamentals with Databricks course.
# MAGIC 
# MAGIC Please proceed to the next lesson to begin Module 2: An Introduction to Data Science.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
