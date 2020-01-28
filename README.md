# domo_api
A wrapper to simplify pydomo allowing Python to interface with DOMO

# quick start:

Setup:
In command prompt/terminal
	
	pip install pydomo
    
Clone or download domo_api.py from

	https://github.com/helpimalive/domo_api
	
Save domo_api.py somewhere, say
	
	C:\Users\somewhere
	
Generate a CLIENT_ID and CLIENT_SECRET at
	
	developer.domo.com/new-client
	
Paste your CLIENT_ID and CLIENT_SECRET in
	
	domo_api.py
	

Use:
When you’re ready to write a python script that uses the domo connector, paste in the lines
	
	import sys
	sys.path.append(r'C:\Users\somewhere’)
	import domo_api

___OR___  
in your environment variables, add

	C:\Users\somewhere 
___OR___ in a directory already in your environment variables put the file
	
	domo_api.py

To create  a dataset use:
	domo_api.create_datasets(
	dataset_name = ‘your desired name',
	description  = ‘your description',
	dataset_df = None #YOUR DATAFRAME HERE,
	csv_file_path = None #OR YOUR CSV FILEPATH HERE,
	)

To update a dataset use:

	domo_api.update_dataset(
	dataset_name = ‘name of dataset’,
	dataset_df = None #YOUR DATAFRAME HERE,
	csv_file_path = None #OR YOUR CSV FILEPATH HERE,
	)

To retrieve a dataset:

	df = domo_api.export_dataset(
	dataset_name = ‘name of dataset’,
	csv_file_path = None #OR CSV DESTINATION,
	)
