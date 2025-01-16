# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# MAGIC %md
# MAGIC # SP means Speciality Pharmacy
# MAGIC
# MAGIC **Reading and saving speciality pharmacy data from Snowflake to HiveStore in Databricks for faster data reading**

# COMMAND ----------

sp_data = get_data_snowflake(
f"""
  select *      

  from PHCDW.PHCDW_STG_HEM.STG_PRM_KOG_PTNT_SLS
"""
)
print(sp_data.count(), len(sp_data.columns))
display(sp_data.limit(15))

# COMMAND ----------

save_sdf(sp_data, 'heme_data', 'sp_data')
