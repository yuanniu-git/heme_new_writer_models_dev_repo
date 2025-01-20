# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to pre-process and set up the Overlap data to be used for feature engineering, downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ### We use the new Overlap data staged in the common Databricks Hivestore. This data is in raw form and requires further processing to create features for machine learning

# COMMAND ----------

# Importing packages
from pyspark.sql import functions as F  # Importing functions from pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import Row
import pandas as pd

# COMMAND ----------

# importing the helper functions
%run "/Workspace/Repos/yuan.niu@bayer.com/heme_new_writer_models_dev_repo/02_data_processing/helper_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting variable values to be used later

# COMMAND ----------

start_month = "2019-12"
end_month = "2024-11"

# COMMAND ----------

# Shortlisting columns which are useful for data analysis. Generally we exclude columns which have a high percentage of null values.
col_shortlist = [
    "BH_ID",
    "PATIENT_ID",
    "SHP_DT",
    "WINNING_PATIENT_ID",
    "SP_SOURCE_PTNT_ID",
    "SHS_SOURCE_PTNT_ID",
    "PRD_NM",
    "DRUG_NM",
    "PRD_GRP_NM",
    "MKT_NM",
    "DRUG_STRG_QTY",
    "IU",
    "SRC_SP",
    "SOURCE_TYPE",
    "BRTH_YR",
    "SP_PTNT_BRTH_YR",
    "PTNT_AGE_GRP",
    "PTNT_WGT",
    "PTNT_GENDER",
    "PTNT_GNDR",
    "ETHNC_CD",
    "EPSDC",
    "SEVRTY",
    "PRPHY",
    "INSN_ID",
    "INSN_NM",
    "AFFL_TYP",
    "SPCL_CD",
    "DATE_ID",
    "MTH_ID",
    "PAYR_NM",
    "PAYR_TYP",
    "PAY_TYP_CD",
    "COPAY_AMT",
    "TOTL_PAID_AMT",
    "CLAIM_TYP",
    "PRESCRIBED_UNIT",
    "DAYS_SUPPLY_CNT",
    "REFILL_AUTHORIZED_CD",
    "FILL_DT",
    "ELIG_DT",
]

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading in the new Overlap data. This raw input data is same for data analysis of Jivi and Kovaltry. The read data is Spark Dataframe**

# COMMAND ----------

overlap_raw_data = spark.sql("SELECT * FROM heme_data.overlap_rx")
print(
    "Row count: ",
    overlap_raw_data.count(),
    "Column Count: ",
    len(overlap_raw_data.columns),
)

# COMMAND ----------

# Checking duplicate records in original dataframe if there are any
duplicate_records = (
    overlap_raw_data.groupBy(overlap_raw_data.columns).count().filter("count > 1")
)
display(duplicate_records)

# COMMAND ----------

# Converting the original overlap data spark dataframe to pandas dataframe
""" Convert DecimalType columns to float to avoid UserWarning: The conversion of DecimalType columns is inefficient and may take a long time. Column names: [IU, PTD_FNL_CLM_AMT] If those columns are not necessary, you may consider dropping them or converting to primitive types before the conversion."""
overlap_raw_data = overlap_raw_data.withColumn(
    "IU", overlap_raw_data["IU"].cast("float")
)

# COMMAND ----------

# Selecting a subset of columns of overlap data based on columns shortlist
overlap_subset = overlap_raw_data.select(col_shortlist)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing pre-processed Overlap dataset to Hivestore

# COMMAND ----------

save_sdf(overlap_rx, 'heme_data', 'overlap_preprocessed')
