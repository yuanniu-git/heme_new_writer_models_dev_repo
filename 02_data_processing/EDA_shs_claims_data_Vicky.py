# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# MAGIC %run "./helper_functions"

# COMMAND ----------

shs_dx = spark.sql('select * from heme_data.shs_dx')
shs_rx = spark.sql('select * from heme_data.shs_rx')
shs_px = spark.sql('select * from heme_data.shs_px')
overlap_rx = spark.sql('select * from heme_data.overlap_rx')

# COMMAND ----------

print ("max shipping date", shs_rx.agg(max('KOGRX_FILL_DT').alias('max_SHP_DT')).show())
print ("min shipping date", shs_rx.agg(min('KOGRX_FILL_DT').alias('min_SHP_DT')).show())

# COMMAND ----------

shs_rx.filter(col("KOGRX_FILL_DT")>='2023-01-01').select('KOGRX_PTNT_ID').distinct().count()

# COMMAND ----------

shs_rx.filter(col("KOGRX_FILL_DT")>='2023-01-01').select('KOGRX_HCP_ID').distinct().count()

# COMMAND ----------

overlap_rx.select("PRD_NM",'SHP_DT','PTNT_ID','PATIENT_ID','BH_ID').show(5)

# COMMAND ----------

shs_rx.select("KOGRX_DRG_NM",'KOGRX_FILL_DT','KOGRX_PTNT_ID','KOGRX_HCP_ID').show(5)

# COMMAND ----------

shs_rx.show()

# COMMAND ----------

shs_px.show(5)

# COMMAND ----------

sdf_diag.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA Q2

# COMMAND ----------

shs_dx.select('KOGDIAG_DIAG_CD').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA Q3

# COMMAND ----------

shs_px.select('KOGPROC_CD','KOGPROC_DESC').distinct().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA Q4

# COMMAND ----------

overlap_shs_rx = overlap_rx.join(shs_rx,overlap_rx.SHS_SOURCE_PTNT_ID == shs_rx.KOGRX_PTNT_ID,'inner')

# COMMAND ----------

overlap_shs_rx_joined = overlap_rx.join(shs_rx,(overlap_rx.SHS_SOURCE_PTNT_ID == shs_rx.KOGRX_PTNT_ID)&(overlap_rx.SHS_SOURCE_CUST_ID == shs_rx.KOGRX_HCP_ID),'inner').cache()

# COMMAND ----------

## number of records where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS
print("Number of records where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS: ",overlap_shs_rx.count())
print("Number of unique patients where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS: ",overlap_shs_rx.select("SHS_SOURCE_PTNT_ID").distinct().count())


# COMMAND ----------

## number of records where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS
print("Number of records where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS: ",overlap_shs_rx_joined.count())
print("Number of unique patients where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS: ",overlap_shs_rx_joined.select("SHS_SOURCE_PTNT_ID").distinct().count())
print("Number of unique patients where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS: ",overlap_shs_rx_joined.select("KOGRX_PTNT_ID").distinct().count())
print("Number of unique HCP where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_HCP_ID in SHS: ",overlap_shs_rx_joined.select("KOGRX_HCP_ID").distinct().count())
print("Number of unique HCP where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_HCP_ID in SHS: ",overlap_shs_rx_joined.select("SHS_SOURCE_CUST_ID").distinct().count())

# COMMAND ----------

two_yr_overlap_data = overlap_shs_rx_joined.filter(overlap_shs_rx_joined['SHP_DT'] >= '2023-01-01')

# COMMAND ----------

print("Number of records where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS since 2023 Jan: ",two_yr_overlap_data.count())
print("Number of unique patients (SHS_SOURCE_PTNT_ID cnt) where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS since 2023 Jan: ",two_yr_overlap_data.select("SHS_SOURCE_PTNT_ID").distinct().count())
print("Number of unique HCP (KOGRX_HCP_ID cnt) where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_HCP_ID in SHS since 2023 Jan: ",two_yr_overlap_data.select("KOGRX_HCP_ID").distinct().count())

# COMMAND ----------

print("Number of unique patients (PATIENT_ID cnt) where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_PTNT_ID in SHS since 2023 Jan: ",two_yr_overlap_data.select("PATIENT_ID").distinct().count())
print("Number of unique HCP (BH_ID cnt) where SHS_SOURCE_PTNT_ID in Overlap matches with KOGRX_HCP_ID in SHS since 2023 Jan: ",two_yr_overlap_data.select("BH_ID").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA Q6

# COMMAND ----------

shs_df = shs_rx.select('KOGRX_PTNT_ID','KOGRX_FILL_DT','KOGRX_DRG_NM').toPandas()

# COMMAND ----------

shs_df.head()

# COMMAND ----------

  # Convert date_col to datetime
  shs_df['KOGRX_FILL_DT'] = pd.to_datetime(shs_df['KOGRX_FILL_DT'])

# COMMAND ----------

jivi_new_pat = calculate_new_to_drug(shs_df, 'JIVI', 'KOGRX_DRG_NM', '2023-01-01', 'KOGRX_FILL_DT', 'KOGRX_PTNT_ID',2)
jivi_new_pat

# COMMAND ----------

sp_data = spark.sql('''
  SELECT *
  FROM jivi_new_writer_model.sp_data
''')
sp_data = sp_data.cache()
print(sp_data.count())

# COMMAND ----------

sp_data.display()

# COMMAND ----------

df = sp_data.select('KOGPTNT_ID','KOGPTNT_SHP_DT','KOGPTNT_DRG_NM').toPandas()
df.head()
df_jivi = df.copy()
print(df_jivi.shape)
df_jivi = df_jivi[df_jivi['KOGPTNT_DRG_NM']=='JIVI']
print(df_jivi.shape)
df_jivi = df_jivi.sort_values(by=['KOGPTNT_ID','KOGPTNT_SHP_DT'])

# COMMAND ----------

jivi_pat_since23_df = df_jivi[df_jivi['KOGPTNT_SHP_DT']>='20230101'].drop_duplicates(subset='KOGPTNT_ID', keep='first')
print ("unique Jivi patient count since Jan 23:", jivi_pat_since23_df.shape[0]) #529
jivi_pat_since23_df.rename(columns={'KOGPTNT_SHP_DT':'Index_date'},inplace=True)
jivi_pat_since23_df['Index_date'] = pd.to_datetime(jivi_pat_since23_df['Index_date'], format='%Y%m%d')
jivi_pat_since23_df['2Y_before_index_date'] = jivi_pat_since23_df['Index_date'] - pd.DateOffset(years=2)

# COMMAND ----------

jivi_pat_since23_df.shape

# COMMAND ----------

jivi_pat_since23_df.head()

# COMMAND ----------

df_jivi_rx = df_jivi[['KOGPTNT_ID','KOGPTNT_SHP_DT']].drop_duplicates()
jivi_pat_since23_df = pd.merge(jivi_pat_since23_df,df_jivi_rx, on =['KOGPTNT_ID'],how = 'left')
jivi_pat_since23_df['KOGPTNT_SHP_DT'] = pd.to_datetime(jivi_pat_since23_df['KOGPTNT_SHP_DT'], format='%Y%m%d')

# COMMAND ----------

jivi_pat_since23_df.shape

# COMMAND ----------

jivi_pat_since23_df.head()

# COMMAND ----------

existing_jivi_pat_df = jivi_pat_since23_df.loc[(jivi_pat_since23_df['KOGPTNT_SHP_DT']>=jivi_pat_since23_df['2Y_before_index_date'])&  (jivi_pat_since23_df['KOGPTNT_SHP_DT']<jivi_pat_since23_df['Index_date'])]
existing_jivi_pat_df.shape
existing_jivi_pat_list = existing_jivi_pat_df['KOGPTNT_ID'].unique().tolist()
len(existing_jivi_pat_list) #226

# COMMAND ----------

existing_jivi_pat_df.loc[existing_jivi_pat_df['Index_date']=='2023-01-23']['KOGPTNT_ID'].unique()

# COMMAND ----------

jivi_pat_since23_df.loc[jivi_pat_since23_df['KOGPTNT_ID']=='012C7D85-D64F-47D9-9963-B698FF0E64D2']

# COMMAND ----------

existing_jivi_pat_df

# COMMAND ----------

jivi_new_pat_since23_df = jivi_pat_since23_df.loc[~jivi_pat_since23_df['KOGPTNT_ID'].isin(existing_jivi_pat_list)]
jivi_new_pat_since23_df['Index_date_YM'] = jivi_new_pat_since23_df['Index_date'].dt.strftime('%Y-%m')

# COMMAND ----------

len(jivi_new_pat_since23_df['KOGPTNT_ID'].unique().tolist())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### SP data: New Jivi patient by Month

# COMMAND ----------

jivi_new_pat = calculate_new_to_drug_hcps(df_jivi, 'JIVI', 'KOGPTNT_DRG_NM', '2023-01-01', 'KOGPTNT_SHP_DT', 'KOGPTNT_ID')

# COMMAND ----------

jivi_new_pat

# COMMAND ----------

jivi_new_pat_since23_df.groupby("Index_date_YM")['KOGPTNT_ID'].nunique().reset_index()

# COMMAND ----------

jivi_new_pat_since23_df.head()
