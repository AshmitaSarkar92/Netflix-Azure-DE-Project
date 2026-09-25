# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS netflix_catalog.nextflix_schema;

# COMMAND ----------

checkpoint_location ="abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/checkpoints"


# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://raw@netflixprojectdatalake.dfs.core.windows.net")

# COMMAND ----------

# DBTITLE 1,Cell 4
display(df, checkpointLocation="abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/checkpoints/display_preview")

# COMMAND ----------

print([q.id for q in spark.streams.active])

# COMMAND ----------

df.writeStream\
       .option("checkpointLocation", checkpoint_location)\
       .trigger(availableNow=True)\
       .start("abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/netflix_titles")


# COMMAND ----------

display(spark.read.format("delta").load("abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/netflix_titles"))

# COMMAND ----------

spark.read.format("delta").load("abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/netflix_titles").count()