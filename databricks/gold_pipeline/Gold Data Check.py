# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_directors LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_cast LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_countries LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_category LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_titles_stg LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM netflix_catalog.netflix_elt_schema.gold_netflix_titles LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'directors' AS table_name, COUNT(*) AS row_count FROM netflix_catalog.netflix_elt_schema.gold_netflix_directors
# MAGIC UNION ALL
# MAGIC SELECT 'cast', COUNT(*) FROM netflix_catalog.netflix_elt_schema.gold_netflix_cast
# MAGIC UNION ALL
# MAGIC SELECT 'countries', COUNT(*) FROM netflix_catalog.netflix_elt_schema.gold_netflix_countries
# MAGIC UNION ALL
# MAGIC SELECT 'category', COUNT(*) FROM netflix_catalog.netflix_elt_schema.gold_netflix_category
# MAGIC UNION ALL
# MAGIC SELECT 'titles_stg', COUNT(*) FROM netflix_catalog.netflix_elt_schema.gold_netflix_titles_stg
# MAGIC UNION ALL
# MAGIC SELECT 'titles', COUNT(*) FROM netflix_catalog.netflix_elt_schema.gold_netflix_titles;