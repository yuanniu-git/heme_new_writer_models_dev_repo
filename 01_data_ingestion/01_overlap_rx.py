# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Fixing the date range filters
# Note: The BETWEEN SQL clause is inclusive on both dates
start_date = '2019-12-01'
end_date = '2024-11-30'

# COMMAND ----------

# Get data from Snowflake
# Note: The BETWEEN SQL clause is inclusive on both dates
overlap_rx = get_data_snowflake(
f"""
  SELECT *      
  FROM PHCDW.PHCDW_CDM.TMP_HEM_OVLP_DLT_VW
  WHERE SHP_DT BETWEEN '{start_date}' AND '{end_date}'
"""
)
print(overlap_rx.count(), len(overlap_rx.columns))
display(overlap_rx.limit(15))

# COMMAND ----------

save_sdf(overlap_rx, 'heme_data', 'overlap_rx')
