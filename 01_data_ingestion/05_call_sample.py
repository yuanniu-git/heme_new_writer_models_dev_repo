# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Fixing the date range filters
# Note: The BETWEEN SQL clause is inclusive on both dates
start_date = '2019-12-01'
end_date = '2024-11-30'

# COMMAND ----------

# Note: The BETWEEN SQL clause is inclusive on both dates
# Get data from Snowflake
sdf = get_data_snowflake(
f"""
  SELECT * FROM CPH_DB_PROD.ANALYTICS_V2.ANLT_BASE_FACT_CUST_CALL_SMPL 
  WHERE PROD_BRAND_NM in ('JIVI','KOVALTRY') 
  AND CALL_SMPL_DATE BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(sdf.count(),len(sdf.columns))
# display(sdf.limit(15))

# COMMAND ----------

save_sdf(sdf, 'heme_data', 'call_sample_data')
