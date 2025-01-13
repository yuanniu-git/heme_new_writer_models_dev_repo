# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

calls_data = spark.sql("SELECT * FROM heme_data.call_activity_data")
print('Row count: '+ str(calls_data.count()), 'Column Count: '+ str(len(calls_data.columns)))

# COMMAND ----------

display(calls_data.limit(15))

# COMMAND ----------


