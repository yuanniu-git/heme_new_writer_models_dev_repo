# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Get data from Snowflake
sdf = get_data_snowflake(
f"""
  SELECT * FROM CPH_DB_PROD.ANALYTICS_V2.ANLT_BASE_FACT_CUST_CALL_ACTY
  WHERE PROD_BRAND_NM IN ('JIVI','KOVALTRY','KOGENATE FS') AND CUST_HCP_ID IS NOT NULL
"""
)
print(sdf.count(),len(sdf.columns))

# COMMAND ----------

save_sdf(sdf, 'heme_data', 'call_activity_data')
