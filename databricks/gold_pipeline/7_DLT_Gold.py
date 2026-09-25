# Databricks notebook source
# MAGIC %md
# MAGIC Gold Load

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

lookuptables_rules = {
   "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(name="gold_netflix_directors")
@dlt.expect_all_or_drop(lookuptables_rules)
def gold_netflix_directors():
    df = (spark.readStream
              .format("delta")
              .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_directors"))
    return df

# COMMAND ----------

@dlt.table(name="gold_netflix_cast")
@dlt.expect_all_or_drop(lookuptables_rules)
def gold_netflix_cast():
    df = (spark.readStream
              .format("delta")
              .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_cast"))
    return df

# COMMAND ----------

@dlt.table(name="gold_netflix_countries")
@dlt.expect_all_or_drop(lookuptables_rules)
def gold_netflix_countries():
    df = (spark.readStream
              .format("delta")
              .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_countries"))
    return df

# COMMAND ----------

@dlt.table(name="gold_netflix_category")
@dlt.expect_or_drop("rule1" , "show_id is NOT NULL")
def gold_netflix_category():
    df = (spark.readStream
              .format("delta")
              .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_category"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Create - a staging gold record -> View -> gold record
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_netflix_titles_stg")
def gold_netflix_titles_stg():
    df = (spark.readStream
              .format("delta")
              .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_titles"))
    return df

# COMMAND ----------

@dlt.view(name="gold_netflix_titles_trans")
 def gold_netflix_titles_trans():
    df = spark.readStream.table("gold_netflix_titles_stg")
    df = df.withColumn("new_flag", lit(1))
    return df

# COMMAND ----------

masterdata_rules = {
   "rule1" : "new_flag is NOT NULL",
   "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(name="gold_netflix_titles")
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflix_titles():
    df = spark.readStream.table("gold_netflix_titles_trans")
    return df