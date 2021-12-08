# Databricks notebook source
# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

airbnb_data = spark.read.parquet(input_path)
trainDF, testDF = airbnb_data.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up and run an AutoML experiment
# MAGIC 
# MAGIC We will set up a regression experiment with our AirBnB dataset, with `price` as the target column and RMSE as the metric.

# COMMAND ----------

import databricks.automl as automl

model = automl.regress(
    dataset = trainDF, 
    target_col = "price",
    primary_metric = "rmse",
    timeout_minutes = 5,
    max_trials = 10
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register a Model
# MAGIC 
# MAGIC Once the AutoML experiment is done, we can identify the best model from the experiment and register that model to the MLflow Model Registry.

# COMMAND ----------

import mlflow

client = mlflow.tracking.MlflowClient()

run_id = model.best_trial.mlflow_run_id
model_name = "airbnb-price"
model_uri = f"runs:/{run_id}/model"

model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

print(model_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the model to perform batch inference

# COMMAND ----------

predict = mlflow.pyfunc.spark_udf(spark, model_uri)
predDF = testDF.withColumn("prediction", predict(*testDF.drop("price").columns))
display(predDF)
