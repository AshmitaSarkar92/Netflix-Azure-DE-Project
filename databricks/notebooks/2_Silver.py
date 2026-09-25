# Databricks notebook source
# MAGIC %md
# MAGIC Silver lookup tables

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

var_source_folder= dbutils.widgets.get("sourcefolder")
var_target_folder= dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load(f"abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/{var_source_folder}")


# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
.mode("append")\
.option("path", f"abfss://silver@netflixprojectdatalake.dfs.core.windows.net/{var_target_folder}")\
.save()