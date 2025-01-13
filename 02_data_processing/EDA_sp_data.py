# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/yuan.niu@bayer.com/jivi_new_writer_model_v2025/02_data_processing/helper_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA for Speciality Pharmacy data

# COMMAND ----------

sp_data = spark.sql("SELECT * FROM heme_data.sp_data")
print('Row count: '+ str(sp_data.count()), 'Column Count: '+ str(len(sp_data.columns)))

# COMMAND ----------

display(sp_data.limit(10))

# COMMAND ----------

# Filter the overlap data from Jan 2023 onwards
two_yr_sp_data = sp_data.filter(sp_data['KOGPTNT_SHP_DT'] >= '20230101')
print('Row count: '+ str(two_yr_sp_data.count()))
#display(two_yr_overlap_data)

# COMMAND ----------

# the data ends at which date?
two_yr_sp_data.select(F.min("KOGPTNT_SHP_DT"), F.max("KOGPTNT_SHP_DT")).show()

# COMMAND ----------

# converting to pandas dataframe
two_yr_sp_df = two_yr_sp_data.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Number of unique HCPs and Unique Patients since Jan-2023 till end of data for **Speciality Pharmacy** data table

# COMMAND ----------

print("Unique HCPs: " + str(two_yr_sp_df.KOGPTNT_HCP_ID.nunique()))
print("Unique Patients: " + str(two_yr_sp_df.KOGPTNT_ID.nunique()))

# COMMAND ----------

unique_jivi_hcp_count = two_yr_sp_df.query("KOGPTNT_DRG_NM == 'JIVI'").KOGPTNT_HCP_ID.nunique()
unique_jivi_patient_count = two_yr_sp_df.query("KOGPTNT_DRG_NM == 'JIVI'").KOGPTNT_ID.nunique()
print("Unique JIVI HCPs: ", unique_jivi_hcp_count)
print("Unique JIVI Patients: ", unique_jivi_patient_count)

# COMMAND ----------

unique_kov_hcp_count = two_yr_sp_df.query("KOGPTNT_DRG_NM == 'KOVALTRY'").KOGPTNT_HCP_ID.nunique()
unique_kov_patient_count = two_yr_sp_df.query("KOGPTNT_DRG_NM == 'KOVALTRY'").KOGPTNT_ID.nunique()
print("Unique Kovaltry HCPs: ", unique_kov_hcp_count)
print("Unique Kovaltry Patients: ", unique_kov_patient_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We consider the study period of our data from Jan-2023 to Oct-2024 (end of data)

# COMMAND ----------

# MAGIC %md
# MAGIC EDA Q7: b. Monthly and overall count of 'New-to-Kovaltry' HCPs between Jan'23 and end of data date. 'New to Kovaltry' HCPs are defined as those who 
# MAGIC 1. Prescribed Kovaltry in the study period and 
# MAGIC 2. Didn't write Kovaltry in the 24 months prior

# COMMAND ----------

subset_sp_df = sp_data['KOGPTNT_DRG_NM', 'KOGPTNT_SHP_DT', 'KOGPTNT_HCP_ID', 'KOGPTNT_ID'].toPandas()

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_hcps = calculate_new_to_drug(data=subset_sp_df, drg_nm='KOVALTRY', drg_nm_col='KOGPTNT_DRG_NM', start_date='2023-01-01', id_col='KOGPTNT_HCP_ID', date_col='KOGPTNT_SHP_DT', lookback_period=2)
display(monthly_new_to_drg_hcps)

# COMMAND ----------

# MAGIC %md
# MAGIC Same analysis for **New Kovaltry Patients**

# COMMAND ----------

monthly_new_to_drg_pats = calculate_new_to_drug(data=subset_sp_df, drg_nm='KOVALTRY', drg_nm_col='KOGPTNT_DRG_NM', start_date='2023-01-01', id_col='KOGPTNT_ID', date_col='KOGPTNT_SHP_DT', lookback_period=2)
display(monthly_new_to_drg_pats)

# COMMAND ----------

# MAGIC %md
# MAGIC EDA Q7: a. Monthly and overall count of 'New-to-JIVI' HCPs between Jan'23 and end of data date. 'New to JIVI' HCPs are defined as those who
# MAGIC
# MAGIC 1. Prescribed JIVI in the study period and
# MAGIC 2. Didn't write JIVI in the 24 months prior

# COMMAND ----------

# using the imported helper function
monthly_new_to_drg_hcps = calculate_new_to_drug(data=subset_sp_df, drg_nm='JIVI', drg_nm_col='KOGPTNT_DRG_NM', start_date='2023-01-01', id_col='KOGPTNT_HCP_ID', date_col='KOGPTNT_SHP_DT', lookback_period=2)
display(monthly_new_to_drg_hcps)

# COMMAND ----------



# COMMAND ----------

monthly_new_to_drg_hcps.KOGPTNT_HCP_ID_new.sum()

# COMMAND ----------

monthly_new_to_drg_pats = calculate_new_to_drug(data=subset_sp_df, drg_nm='JIVI', drg_nm_col='KOGPTNT_DRG_NM', start_date='2023-01-01', id_col='KOGPTNT_ID', date_col='KOGPTNT_SHP_DT', lookback_period=2)
display(monthly_new_to_drg_pats)

# COMMAND ----------


