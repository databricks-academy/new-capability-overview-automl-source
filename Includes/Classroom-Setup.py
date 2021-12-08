# Databricks notebook source
# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC 
# MAGIC displayHTML("Initialized classroom variables & functions...")

# COMMAND ----------

# MAGIC %python
# MAGIC course_name = "intro-to-automl"
# MAGIC 
# MAGIC username = spark.conf.get("com.databricks.training.username", "unknown-username")
# MAGIC 
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username)
# MAGIC dbutils.fs.rm("dbfs:/user/" + username + "/" + course_name, True)
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username + "/" + course_name)
# MAGIC 
# MAGIC database_name = username + "_" + course_name
# MAGIC database_name = database_name.replace(".", "_").replace("@", "_")
# MAGIC try:
# MAGIC     spark.sql(f"CREATE DATABASE {database_name}")
# MAGIC except:
# MAGIC     None
# MAGIC 
# MAGIC base_read_path = "wasbs://courseware@dbacademy.blob.core.windows.net/introduction-to-automl/v01/"
# MAGIC london_read_path = base_read_path + "london/"
# MAGIC base_write_path = "dbfs:/user/" + username + "/" + course_name + "/"
# MAGIC 
# MAGIC input_path = london_read_path + "sf-listings-2019-03-06-clean.parquet/"
# MAGIC 
# MAGIC None
