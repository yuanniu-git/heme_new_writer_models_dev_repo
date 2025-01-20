# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Fixing the date range filters
# Note: The BETWEEN SQL clause is inclusive on both dates
start_date = '2019-12-01'
end_date = '2024-11-30'

# COMMAND ----------

# MAGIC %md
# MAGIC # SP means Speciality Pharmacy
# MAGIC
# MAGIC **Reading and saving speciality pharmacy data from Snowflake to HiveStore in Databricks for faster data reading**

# COMMAND ----------

sp_data = get_data_snowflake(
f"""
  SELECT * FROM PHCDW.PHCDW_STG_HEM.STG_PRM_KOG_PTNT_SLS
  WHERE TO_DATE(KOGPTNT_SHP_DT, 'YYYYMMDD') BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(sp_data.count(), len(sp_data.columns))
# display(sp_data.limit(15))

# COMMAND ----------

save_sdf(sp_data, 'heme_data', 'sp_data')
