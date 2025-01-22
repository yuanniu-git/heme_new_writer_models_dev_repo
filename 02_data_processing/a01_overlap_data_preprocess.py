# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to pre-process and set up the Overlap data to be used for feature engineering, downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ### We use the new Overlap data staged in the common Databricks Hivestore. This data is in raw form and requires further processing to create features for machine learning

# COMMAND ----------

# Importing packages
from pyspark.sql import functions as F  # Importing functions from pyspark.sql
from pyspark.sql import Window
from pyspark.sql import Row
import pandas as pd

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/yuan.niu@bayer.com/heme_new_writer_models_dev_repo/02_data_processing/helper_functions"

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

display(overlap_raw_data.limit(15))

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

# Check the date ranges of the retrieved overlap data
overlap_subset.select(F.min("SHP_DT"), F.max("SHP_DT")).show()

# COMMAND ----------

# Convert to Pandas dataframe from Spark dataframe
# overlap_subset_df = overlap_subset.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a year-month column in the original dataframe which is used in downstream analysis various times

# COMMAND ----------

# Convert SHP_DT to date and create a new column SHP_YR_MO with year-month format
overlap_subset = overlap_subset.withColumn("SHP_DT", F.to_date("SHP_DT"))
overlap_subset = overlap_subset.withColumn("SHP_YR_MO", F.date_format("SHP_DT", "yyyy-MM"))

# Reorder columns to place SHP_YR_MO next to SHP_DT
columns = overlap_subset.columns
shp_dt_index = columns.index("SHP_DT")
columns.insert(shp_dt_index + 1, columns.pop(columns.index("SHP_YR_MO")))
overlap_subset = overlap_subset.select(*columns)

# COMMAND ----------

display(overlap_subset.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fill the missing values in some categorical columns to avoid problems in feature engineering step

# COMMAND ----------


# Filling missing values in categorical variables
overlap_subset = overlap_subset.fillna(
  {'EPSDC': 'UNK', 
   'PRPHY': 'UNK'}
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing pre-processed Overlap dataset to Hivestore

# COMMAND ----------

save_sdf(overlap_subset, 'heme_data', 'overlap_preprocessed')
