# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

overlap_rx = get_data_snowflake(
f"""
  select *      

  from PHCDW.PHCDW_CDM.TMP_HEM_OVLP_DLT_VW
"""
)
print(overlap_rx.count(), len(overlap_rx.columns))
overlap_rx.printSchema()

# COMMAND ----------

save_sdf(overlap_rx, 'heme_data', 'overlap_rx')
