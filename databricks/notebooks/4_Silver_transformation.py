# Databricks notebook source
# MAGIC %md
# MAGIC Silver Data Tranformation

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

df = spark.read.format("delta")\
     .option("header", True)\
     .option("inferSchema", True)\
     .load("abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df= df.fillna({"duration_minutes":0, "duration_seasons" :1})

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Cast duration columns to IntegerType
df = df.withColumn("duration_minutes", col('duration_minutes').try_cast(IntegerType()))\
    .withColumn("duration_seasons", col('duration_seasons').try_cast(IntegerType()))
    

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("shorttitle", split(col('title'),':')[0])
display(df)

# COMMAND ----------

df = df.withColumn("rating", split(col('rating'),'-')[0])
display(df)

# COMMAND ----------

df = df.withColumn("type_flag",when(col('type')=='Movie',1)\
                       .when(col('type')=='TV Show',2)\
                       .otherwise(0))
display(df)


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df = spark.sql("""
    SELECT *
    FROM temp_view
""")
df.display()


# COMMAND ----------

df_agg = df.groupBy("type").agg(count('*').alias('total_count'))
df_agg.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_titles")\
    .save()