# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# MAGIC %md
# MAGIC **DX stands for Diagnostic** 

# COMMAND ----------

shs_dx = get_data_snowflake(
f"""
  SELECT * FROM PHCDW.PHCDW_STG_HEM.STG_SHS_HEM_KOG_DIAG
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
"""
)
print(shs_px.count(), len(shs_px.columns))

display(shs_px.limit(15))

# COMMAND ----------

save_sdf(shs_px, 'heme_data', 'shs_px')
