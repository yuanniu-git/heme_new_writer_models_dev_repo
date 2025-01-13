# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# Load Overlap claims
overlap_rx = spark.sql('select * from heme_data.overlap_rx')

# COMMAND ----------

overlap_df = overlap_rx.select("PATIENT_ID",'PRD_NM','SHP_DT').toPandas()

# COMMAND ----------



# COMMAND ----------

overlap_rx.select("DIAGNOSIS_CD",'DIAGNOSIS_DESC').distinct().display()

# COMMAND ----------

## load HCP dimension
hcp_dim =  get_data_snowflake(
f"""
  select *      

  from PHCDW.PHCDW_CDM.PUB_MDM_HEM_HCP_FLAT_VW
"""
)
print(hcp_dim.count(), len(hcp_dim.columns))
hcp_dim.printSchema()

# COMMAND ----------

## load Product dimension
prod_dim =  get_data_snowflake(
f"""
  select *      

  from PHCDW.PHCDW_CDM.PUB_HEM_BAYER_PROD_DIM_VW
"""
)
print(prod_dim.count(), len(prod_dim.columns))
prod_dim.printSchema()

# COMMAND ----------

prod_dim.filter(col("PROD_BRND_NM")=='JIVI').display()

# COMMAND ----------

prod_dim.select("MDM_PROD_MSTR_ID",'PROD_MSTR_CD','PROD_MSTR_NM','PROD_BRND_NM','PROD_BRND_CD').distinct().display()

# COMMAND ----------

## load Patient dimension
ptnt_dim =  get_data_snowflake(
f"""
  select *      

  from PHCDW.PHCDW_CDM.PUB_HEM_PTNT_DIM_VW
"""
)
print(ptnt_dim.count(), len(ptnt_dim.columns))
ptnt_dim.printSchema()

# COMMAND ----------

ptnt_dim.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overlap data eda

# COMMAND ----------

print ('unique patients: ', overlap_rx.select('PTNT_ID').distinct().count())
print ('unique hcps: ', overlap_rx.select('BH_ID').distinct().count())

# COMMAND ----------

overlap_rx.columns

# COMMAND ----------

print ('unique patients: ', overlap_rx.select('PTNT_ID').distinct().count())
print ('unique hcps: ', overlap_rx.select('BH_ID').distinct().count())

# COMMAND ----------

overlap_rx.filter(col("SHP_DT")>='2023-01-01').select('SHS_SOURCE_PTNT_ID').distinct().count()

# COMMAND ----------

overlap_rx.filter(col("SHP_DT")>='2023-01-01').select('SHS_SOURCE_CUST_ID').distinct().count()

# COMMAND ----------

overlap_rx.filter(col("SHP_DT")>='2023-01-01').select('SP_SOURCE_PTNT_ID').distinct().count()

# COMMAND ----------

overlap_rx.filter(col("SHP_DT")>='2023-01-01').select('SP_SOURCE_CUST_ID').distinct().count()

# COMMAND ----------


hcp_result = overlap_rx.groupBy('BH_ID').agg(
    F.count_distinct('PATIENT_ID').alias('PATIENT_ID_count'),
    F.count_distinct('PTNT_ID').alias('PTNT_ID_count')
)

# COMMAND ----------

print ("max shipping date", overlap_rx.agg(max('SHP_DT').alias('max_SHP_DT')).show())
print ("min shipping date", overlap_rx.agg(min('SHP_DT').alias('min_SHP_DT')).show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join with hcp dim and eda

# COMMAND ----------

overlap_rx_wi_hcp_dim = overlap_rx.join(hcp_dim, overlap_rx.BH_ID == hcp_dim.MDM_HCP_BHID, 'left')

# COMMAND ----------

print("#of unique hcps that have non-empty first names/dim data: ", overlap_rx_wi_hcp_dim.filter(col("HCP_FRMT_NM").isNotNull()).select('BH_ID').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### join with prod dim and eda

# COMMAND ----------

overlap_rx_wi_prod_dim = overlap_rx.join(prod_dim, overlap_rx.PRD_NM == prod_dim.PROD_BRND_NM, 'left')

# COMMAND ----------

## unique JIVI patient and HCP counts
overlap_rx_wi_prod_dim.filter(col("PROD_BRND_NM")=='JIVI').agg(countDistinct('BH_ID'),countDistinct('PTNT_ID')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### join with patient dim and EDA

# COMMAND ----------

ptnt_dim.columns

# COMMAND ----------

overlap_rx_wi_ptnt_dim = overlap_rx.join(ptnt_dim, overlap_rx.PATIENT_ID == ptnt_dim.SRC_PTNT_ID, 'left')

# COMMAND ----------

print("#of unique patients that have non-empty PTNT_DOB/dim data: ", overlap_rx_wi_ptnt_dim.filter(col("PTNT_DOB").isNotNull()).select('PTNT_DOB').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA Q6

# COMMAND ----------

overlap_df = 

# COMMAND ----------

jivi_new_pat = calculate_new_to_drug(sp_df, 'JIVI', 'KOGPTNT_DRG_NM', '2023-01-01', 'KOGPTNT_SHP_DT', 'KOGPTNT_ID',2)
jivi_new_pat

