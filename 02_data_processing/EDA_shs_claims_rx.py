# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/yuan.niu@bayer.com/jivi_new_writer_model_v2025/02_data_processing/helper_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## # EDA for SHS Claims Rx data in PHCDW.PHCDW_STG_HEM.STG_SHS_HEM_KOG_SLS

# COMMAND ----------

shs_rx = spark.sql("SELECT * FROM heme_data.shs_rx")
print('Row count: '+ str(shs_rx.count()), 'Column Count: '+ str(len(shs_rx.columns)))

# COMMAND ----------

display(shs_rx.limit(50))

# COMMAND ----------

# Filter the overlap data from Jan 2023 onwards
two_yr_shs_rx = shs_rx.filter(shs_rx['KOGRX_FILL_DT'] >= '20230101')
print('Row count: '+ str(two_yr_shs_rx.count()))
#display(two_yr_overlap_data)

# COMMAND ----------

# the data ends at which date?
two_yr_shs_rx.select(F.min("KOGRX_FILL_DT"), F.max("KOGRX_FILL_DT")).show()

# COMMAND ----------

# converting to pandas dataframe
two_yr_shs_rx_df = two_yr_shs_rx.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Number of unique HCPs and Unique Patients since Jan-2023 till end of data for **Symphony Prescriptions Claims** data table

# COMMAND ----------

print("Unique HCPs: " + str(two_yr_shs_rx_df.KOGRX_HCP_ID.nunique()))
print("Unique Patients: " + str(two_yr_shs_rx_df.KOGRX_PTNT_ID.nunique()))

# COMMAND ----------

unique_jivi_hcp_count = two_yr_shs_rx_df.query("KOGRX_DRG_NM == 'JIVI'").KOGRX_HCP_ID.nunique()
unique_jivi_patient_count = two_yr_shs_rx_df.query("KOGRX_DRG_NM == 'JIVI'").KOGRX_PTNT_ID.nunique()
print("Unique JIVI HCPs: ", unique_jivi_hcp_count)
print("Unique JIVI Patients: ", unique_jivi_patient_count)

# COMMAND ----------

unique_kov_hcp_count = two_yr_shs_rx_df.query("KOGRX_DRG_NM == 'KOVALTRY'").KOGRX_HCP_ID.nunique()
unique_kov_patient_count = two_yr_shs_rx_df.query("KOGRX_DRG_NM == 'KOVALTRY'").KOGRX_PTNT_ID.nunique()
print("Unique Kovaltry HCPs: ", unique_kov_hcp_count)
print("Unique Kovaltry Patients: ", unique_kov_patient_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We consider the study period of our data from Jan-2023 to Oct-2024 (end of data)

# COMMAND ----------

# MAGIC %md
# MAGIC EDA Q7: a. Monthly and overall count of 'New-to-JIVI' HCPs between Jan'23 and end of data date. 'New to JIVI' HCPs are defined as those who
# MAGIC
# MAGIC 1. Prescribed JIVI in the study period and
# MAGIC 2. Didn't write JIVI in the 24 months prior

# COMMAND ----------

subset_shs_rx = shs_rx['KOGRX_DRG_NM', 'KOGRX_FILL_DT', "KOGRX_HCP_ID", "KOGRX_PTNT_ID"].toPandas()

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_hcps = calculate_new_to_drug(data=subset_shs_rx, drg_nm='JIVI', drg_nm_col='KOGRX_DRG_NM', start_date='2023-01-01', id_col='KOGRX_HCP_ID', date_col='KOGRX_FILL_DT', lookback_period=2)
display(monthly_new_to_drg_hcps)

# COMMAND ----------

# MAGIC %md
# MAGIC Same calculations for **New JIVI Patients**

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_pats = calculate_new_to_drug(data=subset_shs_rx, drg_nm='JIVI', drg_nm_col='KOGRX_DRG_NM', start_date='2023-01-01', id_col='KOGRX_PTNT_ID', date_col='KOGRX_FILL_DT', lookback_period=2)
display(monthly_new_to_drg_pats)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC EDA Q7: b. Monthly and overall count of 'New-to-Kovaltry' HCPs between Jan'23 and end of data date. 'New to Kovaltry' HCPs are defined as those who 
# MAGIC 1. Prescribed Kovaltry in the study period and 
# MAGIC 2. Didn't write Kovaltry in the 24 months prior

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_hcps = calculate_new_to_drug(data=subset_shs_rx, drg_nm='KOVALTRY', drg_nm_col='KOGRX_DRG_NM', start_date='2023-01-01', id_col='KOGRX_HCP_ID', date_col='KOGRX_FILL_DT', lookback_period=2)
display(monthly_new_to_drg_hcps)

# COMMAND ----------

# MAGIC %md
# MAGIC Same analysis for **New Kovaltry Patients**

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_pats = calculate_new_to_drug(data=subset_shs_rx, drg_nm='KOVALTRY', drg_nm_col='KOGRX_DRG_NM', start_date='2023-01-01', id_col='KOGRX_PTNT_ID', date_col='KOGRX_FILL_DT', lookback_period=2)
display(monthly_new_to_drg_pats)
