# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Fixing the date range filters
# Note: The BETWEEN SQL clause is inclusive on both dates
start_date = '2019-12-01'
end_date = '2024-11-30'

# COMMAND ----------

# MAGIC %md
# MAGIC **DX stands for Diagnostic** 

# COMMAND ----------

shs_dx = get_data_snowflake(
f"""
  SELECT * FROM PHCDW.PHCDW_STG_HEM.STG_SHS_HEM_KOG_DIAG
  WHERE TO_DATE(KOGDIAG_DIAG_DT, 'MMDDYYYY') BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(shs_dx.count(), len(shs_dx.columns))
display(shs_dx.limit(15))

# COMMAND ----------

save_sdf(shs_dx, 'heme_data', 'shs_dx')

# COMMAND ----------

shs_rx = get_data_snowflake(
f"""
  SELECT * FROM PHCDW.PHCDW_STG_HEM.STG_SHS_HEM_KOG_SLS
  WHERE TO_DATE(KOGRX_FILL_DT, 'YYYYMMDD') BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(shs_rx.count(), len(shs_rx.columns))

display(shs_rx.limit(15))

# COMMAND ----------

save_sdf(shs_rx, 'heme_data', 'shs_rx')

# COMMAND ----------

shs_px = get_data_snowflake(
f"""
  SELECT * FROM PHCDW.PHCDW_STG_HEM.STG_SHS_HEM_KOG_PROC
  WHERE TO_DATE(KOGPROC_DT, 'YYYYMMDD') BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(shs_px.count(), len(shs_px.columns))

display(shs_px.limit(15))

# COMMAND ----------

save_sdf(shs_px, 'heme_data', 'shs_px')
