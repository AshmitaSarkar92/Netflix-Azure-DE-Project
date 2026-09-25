# Databricks notebook source
# MAGIC %md
# MAGIC Array Parameters for Silver- source and sink
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Array of the files

# COMMAND ----------

files = [
  {
    "sourcefolder" : "netflix_directors",
    "targetfolder" : "netflix_directors"
  },
  {
    "sourcefolder" : "netflix_cast",
    "targetfolder" : "netflix_cast"
  },
  {
    "sourcefolder" : "netflix_countries",
    "targetfolder" : "netflix_countries"
  },
  {
    "sourcefolder" : "netflix_category",
    "targetfolder" : "netflix_category"
  }
]

# COMMAND ----------

# MAGIC %md
# MAGIC Job Utility to Return Array
# MAGIC

# COMMAND ----------

dbutils.jobs.taskValues.set("my_file_array",files)