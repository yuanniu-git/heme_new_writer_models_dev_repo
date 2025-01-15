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
from pyspark.sql import functions as F  # Importing functions from pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import Row
import pandas as pd

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/yuan.niu@bayer.com/heme_new_writer_models_dev_repo/02_data_processing/helper_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting variable values to be used later

# COMMAND ----------

start_month = "2019-12"
end_month = "2024-11"

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

# Selecting columns which are useful for data analysis. Generally we exclude columns which have a high percentage of null values.
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

# Converting the original overlap data spark dataframe to pandas dataframe
""" Convert DecimalType columns to float to avoid UserWarning: The conversion of DecimalType columns is inefficient and may take a long time. Column names: [IU, PTD_FNL_CLM_AMT] If those columns are not necessary, you may consider dropping them or converting to primitive types before the conversion."""
overlap_raw_data = overlap_raw_data.withColumn(
    "IU", overlap_raw_data["IU"].cast("float")
)

# COMMAND ----------

# take a subset of columns of overlap data based on columns shortlist
overlap_subset = overlap_raw_data.select(col_shortlist)

# COMMAND ----------

# Convert to Pandas dataframe from Spark dataframe
overlap_subset_df = overlap_subset.toPandas()

# COMMAND ----------

display(overlap_subset_df.head(10))

# COMMAND ----------

# Check the date ranges of the retrieved overlap data
date_ranges = overlap_subset_df["SHP_DT"].agg(["min", "max"])
display(date_ranges)

# COMMAND ----------

res_df = pandas_data_profiler(overlap_subset_df)
display(res_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a year-month column in the original dataframe which is used in downstream analysis various times

# COMMAND ----------

# Convert SHP_DT to datetime and create a new column SHP_YR_MO with year-month format
overlap_subset_df["SHP_DT"] = pd.to_datetime(overlap_subset_df["SHP_DT"])
overlap_subset_df["SHP_YR_MO"] = overlap_subset_df["SHP_DT"].dt.strftime("%Y-%m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Spine aka Unit of Analysis of the dataset i.e., HCP-Month combination

# COMMAND ----------

# Create a monthly date range from 2019-12 to 2024-11
yr_mo_range = spark.createDataFrame(
    [Row(SHP_YR_MO=mon.strftime('%Y-%m')) for mon in pd.date_range(start='2019-12', end='2024-11', freq='MS')]
)

# COMMAND ----------

# Create a DataFrame with all combinations of hcps and yr_mo_range
hcp_month_pair = (
    overlap_subset.select('BH_ID')
    .distinct()
    .crossJoin(yr_mo_range)
    .orderBy('BH_ID', 'SHP_YR_MO')
)
spine = hcp_month_pair.toPandas()
spine.shape

# COMMAND ----------

spine[["BH_ID", "SHP_YR_MO"]].nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating features for % of Episodic and Prophylactic prescriptions by an HCP

# COMMAND ----------

overlap_subset_df["EPSDC"] = overlap_subset_df["EPSDC"].fillna("UNK")
overlap_subset_df["PRPHY"] = overlap_subset_df["PRPHY"].fillna("UNK")

# COMMAND ----------

def calculate_presc_type_stats(overlap_subset_df):
    """
    Calculate the count and percentage of records with specific conditions in the overlap_subset_df DataFrame.

    This function groups the input DataFrame by 'BH_ID' and 'SHP_YR_MO', then calculates the count of records
    where 'EPSDC' equals "1" and 'PRPHY' equals "1" for each group. It also calculates the total count of records
    for each group. The function then merges these counts and computes the percentage of records with 'EPSDC' and
    'PRPHY' conditions relative to the total count of records in each group.

    Args:
        overlap_subset_df (pd.DataFrame): Input DataFrame containing columns 'BH_ID', 'SHP_YR_MO', 'EPSDC', and 'PRPHY'.

    Returns:
        pd.DataFrame: A DataFrame with columns 'BH_ID', 'SHP_YR_MO', 'EPSDC_cnt', 'PRPHY_cnt', 'records_count',
                      'EPSDC_pct', and 'PRPHY_pct', sorted by 'BH_ID' and 'SHP_YR_MO'.
    """
    # Group by BH_ID and SHP_YR_MO
    grouped = overlap_subset_df.groupby(['BH_ID', 'SHP_YR_MO'])

    # Calculate the count of records with EPSDC=1 for each group
    epsdc_count = grouped['EPSDC'].apply(lambda x: (x == "1").sum()).reset_index(name='EPSDC_cnt')

    # Calculate the count of records with PRPHY=1 for each group
    prphy_count = grouped['PRPHY'].apply(lambda x: (x == "1").sum()).reset_index(name='PRPHY_cnt')

    # Calculate the total count of records for each group
    records_count = grouped.size().reset_index(name='records_count')

    # Merge the counts with the total count
    merged = pd.merge(epsdc_count, prphy_count, on=['BH_ID', 'SHP_YR_MO'])
    merged = pd.merge(merged, records_count, on=['BH_ID', 'SHP_YR_MO'])

    # Calculate the percentages without scaling to 0 to 100 range
    merged['EPSDC_pct'] = (merged['EPSDC_cnt'] / merged['records_count'])
    merged['PRPHY_pct'] = (merged['PRPHY_cnt'] / merged['records_count'])
    
    # Dropping the column because it is not needed in the feature set
    merged.drop('records_count', axis=1, inplace=True) 
    # Sort the result
    merged.sort_values(by=['BH_ID', 'SHP_YR_MO'], inplace=True)
    
    return merged

# COMMAND ----------

presc_type_stats = calculate_presc_type_stats(overlap_subset_df)
print(presc_type_stats.shape)
display(presc_type_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trying the implementation with cross-join method

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Joining the above result set with the previously generated spine
# MAGIC

# COMMAND ----------

# Perform a left join between hcp_month_pair and unique_prd_nm_pivot
spine_presc_type_merged = spine.merge(
    presc_type_stats, on=["BH_ID", "SHP_YR_MO"], how="left"
)

# Fill NAs with zero
spine_presc_type_merged.fillna(0, inplace=True)

# Order by BH_ID and SHP_YR_MO
spine_presc_type_merged.sort_values(by=["BH_ID", "SHP_YR_MO"], inplace=True)

print(spine_presc_type_merged.shape)
display(spine_presc_type_merged)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using rolling function to calculate features for lookback period

# COMMAND ----------

# Ensure the dataframe is sorted by BH_ID and SHP_YR_MO
spine_presc_type_merged.sort_values(by=["BH_ID", "SHP_YR_MO"], inplace=True)

# Store BH_ID and SHP_YR_MO columns
bh_id_shp_yr_mo = spine_presc_type_merged[["BH_ID", "SHP_YR_MO"]]

# Calculate the rolling sum for the past 12 months for each group of BH_ID
rolling_sum = (
  spine_presc_type_merged.groupby('BH_ID', group_keys=False)[['EPSDC_cnt', 'PRPHY_cnt']]
  .apply(lambda x: x.shift(1).fillna(0).rolling(window=12, min_periods=1).sum())
  .reset_index(level=0, drop=True)
)

# Calculate the rolling mean for the past 12 months for each group of BH_ID
rolling_avg = (
  spine_presc_type_merged.groupby('BH_ID', group_keys=False)[['EPSDC_pct', 'PRPHY_pct']]
  .apply(lambda x: x.shift(1).fillna(0).rolling(window=12, min_periods=1).mean())
  .reset_index(level=0, drop=True)
)

# Concatenate BH_ID and SHP_YR_MO columns back to the rolling sum dataframe
rolling_sum = pd.concat([bh_id_shp_yr_mo, rolling_sum], axis=1)
rolling_feats = pd.concat([rolling_sum, rolling_avg], axis=1)

# COMMAND ----------

presc_type_rolling_feats = rolling_feats.copy()
print(presc_type_rolling_feats.shape)
display(presc_type_rolling_feats)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating features for counting Unique drugs by EHL/SHL/NRF class for each HCP for each month in the data

# COMMAND ----------

# Fill missing values in PRD_GRP_NM with 'UNKNOWN'
overlap_subset_df["PRD_GRP_NM"] = overlap_subset_df["PRD_GRP_NM"].fillna("UNKNOWN")
overlap_subset_df["PRD_NM"] = overlap_subset_df["PRD_NM"].fillna("UNKNOWN")

# COMMAND ----------

display(overlap_subset_df[["PRD_NM", "PRD_GRP_NM"]].value_counts(normalize=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Calculate unique drug counts with respect to drug's class for each HCP in each month

# COMMAND ----------

unique_prd_cnt = (
    overlap_subset_df.groupby(["BH_ID", "SHP_YR_MO", "PRD_GRP_NM"])["PRD_NM"]
    .nunique()
    .reset_index()
)

# Rename the column for clarity
unique_prd_cnt = unique_prd_cnt.rename(columns={"PRD_NM": "unique_PRD_NM_cnt"})
display(unique_prd_cnt)

# COMMAND ----------

# Group by BH_ID, SHP_YR_MO, and PRD_GRP_NM, then calculate the number of unique PRD_NM
unique_prd_cnt = (
    overlap_subset_df.groupby(["BH_ID", "SHP_YR_MO", "PRD_GRP_NM"])["PRD_NM"]
    .nunique()
    .reset_index()
)

# Rename the column for clarity
unique_prd_cnt = unique_prd_cnt.rename(columns={"PRD_NM": "unique_PRD_NM_cnt"})

# Pivot the DataFrame
unique_prd_cnt_pivot = unique_prd_cnt.pivot(
    index=["BH_ID", "SHP_YR_MO"], columns="PRD_GRP_NM", values="unique_PRD_NM_cnt"
).reset_index()
unique_prd_cnt_pivot.fillna(0, inplace=True)
# Display the resulting DataFrame
display(unique_prd_cnt_pivot)

# COMMAND ----------

# Perform a left join between hcp_month_pair and unique_prd_nm_pivot
hcp_prd_merged = spine.merge(
    unique_prd_cnt_pivot, on=["BH_ID", "SHP_YR_MO"], how="left"
)

# Fill NAs with zero
hcp_prd_merged.fillna(0, inplace=True)

# Order by BH_ID and SHP_YR_MO
hcp_prd_merged.sort_values(by=["BH_ID", "SHP_YR_MO"], inplace=True)

display(hcp_prd_merged)

# COMMAND ----------

hcp_prd_merged.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Calculate cumulative sum of unique drugs by class in past N months

# COMMAND ----------



# COMMAND ----------

# Ensure the dataframe is sorted by BH_ID and SHP_YR_MO
hcp_prd_merged.sort_values(by=["BH_ID", "SHP_YR_MO"], inplace=True)

# Store BH_ID and SHP_YR_MO columns
bh_id_shp_yr_mo = hcp_prd_merged[["BH_ID", "SHP_YR_MO"]]

# Get all column names except BH_ID and SHP_YR_MO
columns_excluding_spine = [col for col in hcp_prd_merged.columns if col not in ["BH_ID", "SHP_YR_MO"]]

# Calculate the rolling sum for the past 12 months for each group of BH_ID
rolling_sum = (
  hcp_prd_merged.groupby('BH_ID', group_keys=False)[columns_excluding_spine]
  .apply(lambda x: x.shift(1).fillna(0).rolling(window=12, min_periods=1).sum())
  .reset_index(level=0, drop=True)
)

# Concatenate BH_ID and SHP_YR_MO columns back to the rolling sum dataframe
rolling_sum = pd.concat([bh_id_shp_yr_mo.reset_index(drop=True), rolling_sum.reset_index(drop=True)], axis=1)

print(rolling_sum.shape)
display(rolling_sum)

# COMMAND ----------



# COMMAND ----------


drug_class_rolling_sum_feats = rolling_sum.copy()

# COMMAND ----------

hcp_id = "2042C3F9-D"
drug_class_rolling_sum_feats.query("BH_ID == @hcp_id").display()

# COMMAND ----------

hcp_id = "BH10060872"
drug_class_rolling_sum_feats.query("BH_ID == @hcp_id").display()

# COMMAND ----------

hcp_id = "BH10060872"
unique_prd_cnt.query("BH_ID == @hcp_id").display()

# COMMAND ----------

hcp_id = "BH10003313"
drug_class_rolling_sum_feats.query("BH_ID == @hcp_id").display()

# COMMAND ----------

hcp_id = "BH10003313"
unique_prd_cnt.query("BH_ID == @hcp_id").display()

# COMMAND ----------

hcp_id = "BH10003313"
hcp_prd_merged.query("BH_ID == @hcp_id").display()
