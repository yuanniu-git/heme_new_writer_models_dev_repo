# Databricks notebook source
# MAGIC %run "../00_config/set-up"

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA Q6- SP data

# COMMAND ----------

sp_data = spark.sql('''
  SELECT *
  FROM heme_data.sp_data
''')
sp_data = sp_data.cache()
print(sp_data.count())

# COMMAND ----------

sp_data.filter(col("KOGPTNT_SHP_DT")>='2023-01-01').select('KOGPTNT_ID').distinct().count()

# COMMAND ----------

sp_data.filter(col("KOGPTNT_SHP_DT")>='2023-01-01').select('KOGPTNT_HCP_ID').distinct().count()

# COMMAND ----------

256/12181

# COMMAND ----------

sp_df = sp_data.select('KOGPTNT_ID','KOGPTNT_SHP_DT','KOGPTNT_DRG_NM').toPandas()

# COMMAND ----------

  # Convert date_col to datetime
  sp_df['KOGPTNT_SHP_DT'] = pd.to_datetime(sp_df['KOGPTNT_SHP_DT'])

# COMMAND ----------

jivi_new_pat = calculate_new_to_drug(sp_df, 'JIVI', 'KOGPTNT_DRG_NM', '2023-01-01', 'KOGPTNT_SHP_DT', 'KOGPTNT_ID',2)
jivi_new_pat

# COMMAND ----------

def calculate_new_to_drug(data, drg_nm, drg_nm_col, start_date, date_col, id_col,lookback_period):
    """
    Calculate the monthly count of 'New-to-Drug' Healthcare Providers (HCPs) for a specific drug.

    Parameters:
    data (DataFrame): The input data containing drug prescription records.
    drg_nm (str): The name of the drug to analyze.
    drg_nm_col (str): The column name in the data that contains drug names.
    start_date (str): The start date of the study period in 'YYYY-MM-DD' format.
    date_col (str): The column name in the data that contains prescription dates.
    id_col (str): The column name in the data that contains patient IDs or HCP ids.
    lookback_period (numeric): The number of years to look back from the study period.

    Returns:
    DataFrame: A DataFrame with the monthly count of 'New-to-Drug' HCPs.
    """
    # Convert date_col to datetime
    data[date_col] = pd.to_datetime(data[date_col], format='%Y%m%d')
    data = data.sort_values(by=[id_col,date_col])

    # Filter data for drug prescriptions within the study period and keep the first RX date
    first_rx = data[(data[drg_nm_col] == drg_nm) &
                             (data[date_col] >= pd.to_datetime(start_date)) & 
                             (data[date_col] <= data[date_col].max())].drop_duplicates(subset='KOGPTNT_ID', keep='first')
    first_rx.rename(columns={date_col:'Index_date'},inplace=True)
    first_rx['Look_back_start_date'] = first_rx['Index_date'] - pd.DateOffset(years=lookback_period)

   

    # Filter data for drug prescriptions in the 24 months prior to the study period

    df_rx = data[(data[drg_nm_col] == drg_nm)][[id_col,date_col]].drop_duplicates()
    
    # For each patient/hcp in the study period, find if there are prior prescriptions in the lookback period
    first_rx_wi_prev = pd.merge(first_rx,df_rx, on =[id_col],how = 'left')

    # Identify patients who were prescribed with the drug in the study period
    pat_list = first_rx[id_col].unique().tolist()
    
    #Filter for patients who have been prescribed the drug in the lookback period
    existing_pat_df = first_rx_wi_prev.loc[(first_rx_wi_prev[date_col]>=first_rx_wi_prev['Look_back_start_date'])& 
                                    (first_rx_wi_prev[date_col]<first_rx_wi_prev['Index_date'])]
    
     # Identify patients/hcps who were prescribed with the drug in the lookback period
    existing_pat_list = existing_pat_df[id_col].unique().tolist()

    # Identify HCPs who prescribed the drug in the prior period
    #prescribers_prior_period = prior_period_data[hcp_id_col].unique()

    # Identify 'New-to-Brand' Patients/HCPs
    new_to_drg_pat = set(pat_list) - set(existing_pat_list)

    # Filter study period data for 'New-to-Brand' Patients/HCPs
    new_to_drg_data = first_rx[first_rx[id_col].isin(new_to_drg_pat)]

    # Calculate monthly count of 'New-to-Drug' Patients/HCPs
    new_to_drg_data['YR-MO'] = new_to_drg_data['Index_date'].dt.strftime('%Y-%m')
    monthly_new_to_drgs = new_to_drg_data.groupby('YR-MO')[id_col].nunique().reset_index()
    monthly_new_to_drgs = monthly_new_to_drgs.sort_values('YR-MO', ascending=True)

    # Calculate overall count of 'New-to-Drug' Patients/HCPs
    overall_new_to_drg = new_to_drg_data[id_col].nunique()
    total_count_study_period = first_rx[id_col].nunique()

    # Print totals
    print("")
    print("Count of New-to-", drg_nm, " Patients/HCPs in study period: ", overall_new_to_drg)
    print("Total number of Patients/HCPs who were prescribed with", drg_nm, " during study period: ", total_count_study_period)
    print("")
    return monthly_new_to_drgs
