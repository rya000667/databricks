# Databricks notebook source
# MAGIC 
# MAGIC %pip install mlflow

# COMMAND ----------

import pyspark.pandas as ps
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
ps.set_option('compute.ops_on_diff_frames', True)

data = ps.sql("SELECT * FROM default.seattle_housing_data_csv")

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT lot_size_units
# MAGIC FROM default.seattle_housing_data_csv

# COMMAND ----------

inputCols = ["lot_size"]
outputCols = ["lot_size"]

imputer = Imputer(strategy="median", inputCols=inputCols, outputCols=outputCols)

# COMMAND ----------

data = data[data["lot_size"]>0]

# COMMAND ----------


data['lot_size'] = data.lot_size.astype(float)
data['lot_size'] = data.apply(lambda x: x['lot_size'] * 43650 if x['lot_size_units']=='acre' else x['lot_size'], axis=1)
data['lot_size'] = data.lot_size.astype(int)
data['lot_size_units'] = 'sqft'

# COMMAND ----------

spark_data = data.to_spark()

numericalCols = ['beds','baths','size','lot_size']
labelCol = 'price'
stages = []

assembler = VectorAssembler().setInputCols(numericalCols).setOutputCol('numerical_features')
scaler = MinMaxScaler(inputCol=assembler.getOutputCol(), outputCol="scaled_numerical_features")
stages += [assembler, scaler]

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(spark_data)
preppedDataDF = pipelineModel.transform(spark_data)

# COMMAND ----------

display(preppedDataDF)

# COMMAND ----------

trainingData , testData = preppedDataDF.randomSplit([0.7, 0.3])

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow

def train_housing_data(train_data, test_data, label_column, features_column, elastic_net_param, reg_param, max_iter):
    def eval_metrics(predictions):
      evaluator = RegressionEvaluator(
          labelCol=label_column, predictionCol="prediction", metricName="rmse")
      rmse = evaluator.evaluate(predictions)
      evaluator = RegressionEvaluator(
          labelCol=label_column, predictionCol="prediction", metricName="mae")
      mae = evaluator.evaluate(predictions)
      evaluator = RegressionEvaluator(
          labelCol=label_column, predictionCol="prediction", metricName="r2")
      r2 = evaluator.evaluate(predictions)
      return rmse, mae, r2

    with mlflow.start_run():
        lr = LinearRegression(featuresCol="scaled_numerical_features", labelCol=label_column, elasticNetParam=elastic_net_param, regParam=reg_param, maxIter=max_iter)
        lrModel = lr.fit(train_data)
        predictions = lrModel.transform(test_data)
        rmse, mae, r2 = eval_metrics(predictions)
        
        # Print out model metrics
        print("Linear regression model (elasticNetParam=%f, regParam=%f, maxIter=%f):" % (elastic_net_param, reg_param, max_iter))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        # Log hyperparameters for mlflow UI
        mlflow.log_param("elastic_net_param", elastic_net_param)
        mlflow.log_param("reg_param", reg_param)
        mlflow.log_param("max_iter", max_iter)
        # Log evaluation metrics
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)
        # Log the model itself
        mlflow.spark.log_model(lrModel, "model")
        modelpath = "/dbfs/mlflow/taxi_total_amount/model-%f-%f-%f" % (elastic_net_param, reg_param, max_iter)
        mlflow.spark.save_model(lrModel, modelpath)

        # Generate a plot
        image = plot_regression_quality(predictions)

        # Log artifacts (in this case, the regression quality image)
        mlflow.log_artifact("LinearRegressionPrediction.png")
        


# COMMAND ----------

train_housing_data(trainingData, testData, labelCol, "scaled_numerical_features", 0.0, 0.0, 1)

# COMMAND ----------

trainingData

# COMMAND ----------


