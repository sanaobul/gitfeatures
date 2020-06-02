# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE TEST_DB;

# COMMAND ----------

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
# Create date df in pandas
def create_date_table(start='2020-01-01', end='2020-03-01'):
  df = pd.DataFrame({"date": pd.date_range(start, end)})
  df['day'] = df['date'].dt.weekday_name
  df['date'] = df['date'].dt.date
  df['value'] = np.random.randint(100, size=df.shape[0])
  return df

# COMMAND ----------

date_pdf = create_date_table()
sample_table = spark.createDataFrame(date_pdf)
sample_table.write.format('delta').mode("overwrite").saveAsTable('TEST_DB.sample_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEST_DB.sample_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM TEST_DB.sample_table

# COMMAND ----------

sample_table.write.format('delta').mode("overwrite").save("/delta_tables")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.getContext.notebookPath

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls "dbfs:/delta_tables/"

# COMMAND ----------

deltaDataPath = "dbfs:/delta_tables/"
sqlCmd = "SELECT count(*) FROM delta.`{}` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS customer_data_delta 
  USING DELTA 
  LOCATION '{}' 
""".format(deltaDataPath))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM TEST_DB.customer_data_delta

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables/"

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM delta.`{}` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

# COMMAND ----------

print(dbutils.fs.head(deltaDataPath + "/_delta_log/00000000000000000000.json"))

# COMMAND ----------

sqlCmd = "DESCRIBE DETAIL delta.`{}` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS TEST_db.customer_data_delta 
  USING DELTA 
  LOCATION '{}' 
""".format(deltaDataPath))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEST_db.customer_data_delta 

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS TEST_db.customer_data_delta 
  USING DELTA 
  LOCATION '{}' 
""".format(deltaDataPath))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE TEST_db.events (
# MAGIC   date DATE,
# MAGIC   eventId STRING,
# MAGIC   eventType STRING,
# MAGIC   data STRING)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS TEST_db.delta_events
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/delta_tables/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from TEST_db.delta_events

# COMMAND ----------

date_pdf1 = create_date_table('2020-05-01', '2020-06-01')
sample_table1 = spark.createDataFrame(date_pdf)
sample_table1.write.format('delta').mode("append").save("/delta_tables")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/delta_tables/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from TEST_db.delta_events

# COMMAND ----------

spark.read.format("delta").load("dbfs:/delta_tables/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/delta_tables`

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM delta.`{}` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEST_db.delta_events VERSION AS OF version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEST_db.delta_events TIMESTAMP AS OF timestamp_expression

# COMMAND ----------

df2 = spark.read.format("delta").option("versionAsOf", version).load("dbfs:/delta_tables/")
display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEST_db.delta_events@v1

# COMMAND ----------

