# Databricks notebook source
# MAGIC %md
# MAGIC # In this notebook the Unit of Analysis (UOA) is BH_ID (HCP ID) and Time i.e., Shipment Date

# COMMAND ----------

# MAGIC %md
# MAGIC ### We use the new Overlap data staged in the common Databricks Hivestore. This data is in raw form and requires further processing to create variables for data analysis and features for machine learning

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading in the new Overlap data. This raw input data is same for data analysis for Jivi and Kovaltry**

# COMMAND ----------

# Importing packages
from pyspark.sql import functions as F # Importing functions from pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql import Window
import pandas as pd

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/yuan.niu@bayer.com/heme_new_writer_models_dev_repo/02_data_processing/helper_functions"

# COMMAND ----------

overlap_raw_data = spark.sql("SELECT * FROM heme_data.overlap_rx")
print('Row count: ', overlap_raw_data.count(), 'Column Count: ', len(overlap_raw_data.columns))

# COMMAND ----------

# Checking duplicate records in original dataframe if there are any
duplicate_records = overlap_raw_data.groupBy(overlap_raw_data.columns).count().filter("count > 1")
display(duplicate_records)

# COMMAND ----------

# Selecting columns which are useful for data analysis. Generally we exclude columns which have a high percentage of null values.
col_shortlist = [
  'BH_ID',
  'PATIENT_ID',
  'SHP_DT',
  'WINNING_PATIENT_ID',
  'SP_SOURCE_PTNT_ID',
  'SHS_SOURCE_PTNT_ID',
  'PRD_NM',
  'DRUG_NM',
  'PRD_GRP_NM',
  'MKT_NM',
  'DRUG_STRG_QTY',
  'IU',
  'SRC_SP',
  'SOURCE_TYPE',
  'BRTH_YR',
  'SP_PTNT_BRTH_YR',
  'PTNT_AGE_GRP',
  'PTNT_WGT',
  'PTNT_GENDER',
  'PTNT_GNDR',
  'ETHNC_CD',
  'EPSDC',
  'SEVRTY',
  'PRPHY',
  'INSN_ID',
  'INSN_NM',
  'AFFL_TYP',
  'SPCL_CD',
  'DATE_ID',
  'MTH_ID',
  'PAYR_NM',
  'PAYR_TYP',
  'PAY_TYP_CD',
  'COPAY_AMT',
  'TOTL_PAID_AMT',
  'CLAIM_TYP',
  'PRESCRIBED_UNIT',
  'DAYS_SUPPLY_CNT',
  'REFILL_AUTHORIZED_CD',
  'FILL_DT',
  'ELIG_DT',       
                  
]

# COMMAND ----------

# take a subset of columns of overlap data based on columns shortlist
overlap_subset = overlap_raw_data.select(col_shortlist)

# COMMAND ----------

# Converting the original overlap data spark dataframe to pandas dataframe
""" Convert DecimalType columns to float to avoid UserWarning: The conversion of DecimalType columns is inefficient and may take a long time. Column names: [IU, PTD_FNL_CLM_AMT] If those columns are not necessary, you may consider dropping them or converting to primitive types before the conversion."""
overlap_raw_data = overlap_raw_data.withColumn("IU", overlap_raw_data["IU"].cast("float"))

# COMMAND ----------

# Convert to Pandas dataframe from Spark dataframe
overlap_subset_df = overlap_subset.toPandas()

# COMMAND ----------

overlap_subset_df.head()

# COMMAND ----------

# Check the date ranges of the retrieved overlap data
date_ranges = overlap_subset_df['SHP_DT'].agg(['min', 'max'])
display(date_ranges)

# COMMAND ----------

res_df = pandas_data_profiler(overlap_subset_df)
display(res_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating features for counting Unique drugs by EHL/SHL/NRF class for each HCP for each month in the data

# COMMAND ----------

# Check which drug belongs to which class and proportion of records in data
display(overlap_subset_df[['PRD_NM','PRD_GRP_NM']].value_counts(normalize=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate unique drug count in the rolling lookback window per HCP per Month

# COMMAND ----------

# define the look-back period
look_back_window = 12

# COMMAND ----------

# Fill missing values in PRD_GRP_NM and PRD_NM with 'UNKNOWN'
overlap_subset = overlap_subset.fillna({"PRD_GRP_NM": "UNKNOWN", "PRD_NM": "UNKNOWN"})

# COMMAND ----------

# Calculate the number of unique drugs prescribed by each HCP for each drug class for a given month
overlap_hcp_drug_cnt = (overlap_subset.withColumn("SHP_YR_MO",F.date_format("SHP_DT", "yyyy-MM"))
                                      .filter(col("BH_ID").isNotNull())
                                      .groupby("BH_ID","PRD_GRP_NM","SHP_YR_MO")
                                      .agg(countDistinct("PRD_NM").alias("UNIQUE_DRUGS_cnt")))

# COMMAND ----------

display(overlap_hcp_drug_cnt)

# COMMAND ----------

distinct_YR_MM = overlap_hcp_drug_cnt.select(col('SHP_YR_MO')).distinct()
display(distinct_YR_MM)

# COMMAND ----------

overlap_hcp_drug_cnt_cross_join = (
                        overlap_hcp_drug_cnt.select("BH_ID",'PRD_GRP_NM', 
                        col('SHP_YR_MO').alias("PREV_SHP_YR_MO"),
                        col("UNIQUE_DRUGS_cnt").alias("PREV_UNIQUE_DRUGS_cnt"))
                        .crossJoin(distinct_YR_MM) # cross join with all possible months before the current month
                        .withColumn("month_difference", F.months_between("SHP_YR_MO", "PREV_SHP_YR_MO"))
                        # filter for months within lookback window
                        .filter(col("month_difference") <= look_back_window)
                        .filter(col("month_difference") > 0)
                        .orderBy('BH_ID','PRD_GRP_NM','SHP_YR_MO', 'PREV_SHP_YR_MO', 'month_difference')
                        ).cache()

# COMMAND ----------

overlap_hcp_drug_cnt_cross_join.display()

# COMMAND ----------

# overlap_hcp_drug_cnt_cross_join.filter(col("BH_ID")=='2042C3F9-D').display()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt = (overlap_hcp_drug_cnt_cross_join
                                .groupby("BH_ID", "PRD_GRP_NM", "SHP_YR_MO")
                                .agg(sum('PREV_UNIQUE_DRUGS_cnt')).withColumnRenamed('sum(PREV_UNIQUE_DRUGS_cnt)', f'OVP_UNIQUE_DRUGS_CNT_{look_back_window}M') 
                                                           ).cache()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt.display()

# COMMAND ----------

# Pivot the DataFrame to wide format
overlap_hcp_drug_rolling_cnt_pivot = (overlap_hcp_drug_rolling_cnt.groupBy(["BH_ID", "SHP_YR_MO"])
                                      .pivot("PRD_GRP_NM")
                                      .agg({f'OVP_UNIQUE_DRUGS_CNT_{look_back_window}M': "first"})).cache()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_pivot.display()

# COMMAND ----------

# Rename the columns to add the category names to column names
overlap_hcp_drug_rolling_cnt_pivot = overlap_hcp_drug_rolling_cnt_pivot.select(
    "BH_ID", "SHP_YR_MO",
    *[col(c).alias(f"OVP_UNIQUE_DRUGS_CNT_{c}_{look_back_window}M") for c in overlap_hcp_drug_rolling_cnt_pivot.columns if c not in ["BH_ID", "SHP_YR_MO"]]
).cache()
overlap_hcp_drug_rolling_cnt_pivot = overlap_hcp_drug_rolling_cnt_pivot.fillna(0)

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_pivot.display()

# COMMAND ----------

# count of rows before filling all missing months with 0
overlap_hcp_drug_rolling_cnt_pivot.count()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_pivot.filter(col("BH_ID")=='2042C3F9-D').display()

# COMMAND ----------

hcp_month_pair = (overlap_hcp_drug_rolling_cnt.select("BH_ID").distinct()
                  .crossJoin(distinct_YR_MM)
                  .orderBy('BH_ID','SHP_YR_MO'))

# COMMAND ----------

hcp_month_pair.count()

# COMMAND ----------

unique_bh_id_count = hcp_month_pair.select("BH_ID").distinct().count()
unique_shp_yr_mo_count = hcp_month_pair.select("SHP_YR_MO").distinct().count()

print(f"Unique BH_ID count: {unique_bh_id_count}")
print(f"Unique SHP_YR_MO count: {unique_shp_yr_mo_count}")

# COMMAND ----------

hcp_month_pair.display()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_fillna = (
  hcp_month_pair.join(overlap_hcp_drug_rolling_cnt_pivot,                                                            on = ['BH_ID','SHP_YR_MO'], how = 'left')
  .orderBy('BH_ID','SHP_YR_MO')
  .fillna(0)
  )

# COMMAND ----------

#count of rolls after filling all missing months with 0
overlap_hcp_drug_rolling_cnt_fillna.count()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_fillna.display()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_fillna.filter(col("BH_ID")=='2042C3F9-D').display()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_fillna.filter(col("BH_ID")=='BH10060872').display()

# COMMAND ----------

overlap_hcp_drug_rolling_cnt_fillna.filter(col("BH_ID")=='BH10003313').display()

# COMMAND ----------

overlap_hcp_drug_cnt.filter(col("BH_ID")=='BH10003313').display()
