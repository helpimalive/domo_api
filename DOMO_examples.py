####
# IMPORT THE domo_api MODULE
# The following three lines of uncommented code
# are required, in this order
# you must change this filepath to the location of your domo_api.py
# (the one you downloaded off github)
####

import sys
sys.path.append(r'C:\Users\_FILEPATH_WHERE_YOU_SAVED_DOMO_API')
import domo_api

####
# IMPORT OTHER MODULES AS NEEDED
# These will be required for the examples
####

import pandas as pd
from fredapi import Fred


###
# CREATE DATA SET FROM DATAFRAME
###
domo_api.create_datasets(
	dataset_name = 'DATASET_NAME',
	description  = 'DATASET_DESCRIPTION',
	dataset_df = YOUR_DATAFRAME,
	csv_file_path = None)
###
# CREATE DATA SET FROM CSV
###
domo_api.create_datasets(
	dataset_name = 'DATASET_NAME',
	description  = 'rent_data created by DOMO_examples.py',
	dataset_df = None,
	csv_file_path= 'PATH_TO_THE_FILE\\FILE.CSV')

###
# UPDATE DATASET FROM DATAFRAME
###
domo_api.update_dataset(
	dataset_name = 'DATASET_NAME',
	dataset_df = YOUR_DATAFRAME,
	csv_file_path = None)

###
# UPDATE DATASET FROM CSV
###
domo_api.update_dataset(
	dataset_name = 'DATASET_NAME',
	csv_file_path= 'PATH_TO_THE_FILE\\FILE.CSV')

###
# DELETE A DATASET
###
domo_api.delete_dataset(
	dataset_name = 'DATASET_NAME',
)	

###
# PULL A DATASET INTO A DATAFRAME
###
df = domo_api.export_dataset(
	dataset_name = 'DATASET_NAME',
	csv_file_path= None,
)

###
# EXPORT A DATASET TO A FILEPATH
###
domo_api.export_dataset(
	dataset_name = 'DATASET_NAME',
	csv_file_path= 'PATH_TO_THE_FILE\\FILE.CSV',
)



###
# PULL DATA
# this example uses the FRED API, which connects to the St. Louis Fed's website
# to get a free API key, go to https://research.stlouisfed.org/docs/api/api_key.html
###

def pull_data():
	API_key = 'YOUR_FRED_API_KEY_HERE'
	fred 	= Fred(api_key=API_key)

	home_price = fred.get_series('CSUSHPINSA')
	home_price = pd.DataFrame(home_price)
	print(home_price.head())

	mortgage = fred.get_series('MORTGAGE30US')
	mortgage = pd.DataFrame(mortgage)
	print(mortgage.head())

	data = home_price.merge(mortgage,
	left_index=True,
	right_index=True)

	data.columns = ['home_price','mortgage']

	return(data)


def var_forecasting_example():
	df = pull_data()
	print (df.tail())
	##CONVERT MONTHLY SERIES TO ANNUAL
	df_o = df.groupby(df.index.year).mean()
	print (df_o.tail())
	##TIME SERIES DATA MUST BE DIFFERENCED IN ORDER TO BE ANALYZED
	df = df_o.pct_change()
	print (df.head())
	##THIS WILL ALWAYS CREATE ONE ROW OF NANS 
	##(THE FIRST ROW WHICH CANNOT BE DIFFERENCED)
	df = df.dropna()

	##SPLIT INTO TRAINING AND TEST DATA
	df_train = df[df.index<pd.to_datetime('2016').year]
	df_test = df[df.index>=pd.to_datetime('2016').year]

	##FIT A VECTOR AUTOREGRESSION MODEL
	model = VAR(df_train)
	results = model.fit()

	##FORECAST THE FUTURE VALUES AND THE UPPER AND LOWER CONFIDENCE INTERVAL
	pred = pd.DataFrame(results.forecast_interval(df_train.values,4)[0]+1,
				columns = ['home_price','mortgage']).drop('mortgage',axis=1)
	pred['pred_l'] = pd.DataFrame(results.forecast_interval(df_train.values,4)[1]+1,
				columns = ['home_price','mortgage']).drop('mortgage',axis=1)
	pred['pred_h'] = pd.DataFrame(results.forecast_interval(df_train.values,4)[2]+1,
				columns = ['home_price','mortgage']).drop('mortgage',axis=1)
	print(pred)
	##JOIN THESE FORECAST VALUES TO THE LAST VALUE IN THE TRAIN DATA
	##BECAUSE THEY ARE PERCENT CHANGES AND WE NEED THE LAST VALUE
	##TO MULTIPLY THEM BY
	last_vals = pd.DataFrame([df_o.loc[max(df_train.index)].values],
				columns = df_train.columns).drop('mortgage',axis=1)
	last_vals['pred_l'] = last_vals['home_price']
	last_vals['pred_h'] = last_vals['home_price']
	df_pred = pd.concat([last_vals,pred],axis=0,ignore_index=True)
	df_pred.index = df_o.index[-len(df_pred):]
	df_pred = df_pred.cumprod()
	print(df_pred)
	##JOIN THIS FOREECAST SERIES TO THE ORIGINAL SERIES
	df_pred.rename(columns={'home_price':'home_price_pred'},inplace=True)
	df_final = df_o.merge(df_pred,how='left',left_index=True,right_index=True)
	df_final = df_final.reset_index()

	##UPLOAD DATA
	domo_api.create_datasets(
		dataset_name = 'home_price_and_forecast',
		description  = 'home_price_and_forecast created by DOMO_examples_display.py',
		dataset_df = df_final,
		csv_file_path= None)
