import logging
import numpy as np
import pandas as pd
from pydomo import Domo
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting
from pydomo.users import CreateUserRequest
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
from pydomo.streams import UpdateMethod, CreateStreamRequest
from pydomo.groups import CreateGroupRequest

####
# YOUR CREDENTIALS GO BELOW
# If you need credentials, go to 
# developer.domo.com/new-client
####

CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
API_HOST = 'api.domo.com'

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)
domo = Domo(CLIENT_ID, CLIENT_SECRET, logger_name='foo', log_level=logging.INFO, api_host=API_HOST)
ds = list(domo.datasets.list())
ds_dict={x['name']:x['id'] for x in ds}

def create_datasets(dataset_name,
                    description,
                    dataset_df=None,
                    csv_file_path=None,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                    API_HOST=API_HOST,
                    domo=domo,
                    ds_dict =ds_dict):
    domo.logger.info("\n**** Running a Domo API Instance ****\n")
    datasets = domo.datasets

    if dataset_name in ds_dict.keys():
        raise Exception('Dataset already exists \n consider update_dataset()')

    if dataset_df is None and csv_file_path is None:
        raise Exception('both dataset_df and csv_file_path cannot be None')

    
    dsr = DataSetRequest()
    dsr.name = dataset_name
    dsr.description = description

    if dataset_df is None:
        dataset_df = pd.read_csv(csv_file_path)

    # Define a dataset_df Schema
    schema  = []
    for col in dataset_df.columns:
        try:
            if type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==np.float64:
                WHAT_TYPE=ColumnType.DECIMAL
            elif type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==str:
                WHAT_TYPE=ColumnType.STRING
            elif type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==pd.Timestamp:
                WHAT_TYPE=ColumnType.DATETIME
            else:
                WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
        except:
            WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
            
    dsr.schema= Schema(schema)
    created_dataset = datasets.create(dsr)
    domo.logger.info("Created dataset_df " + created_dataset['id'])
    ds_dict[dataset_name]=created_dataset['id']

    update_dataset(
        dataset_name,
        dataset_df=dataset_df,
        csv_file_path=csv_file_path,
        CLIENT_ID=CLIENT_ID,
        CLIENT_SECRET=CLIENT_SECRET,
        API_HOST=API_HOST
        )

def delete_dataset(dataset_name,
                    CLIENT_ID=CLIENT_ID,
                    CLIENT_SECRET=CLIENT_SECRET,
                    API_HOST=API_HOST,
                    domo=domo,
                    ds_dict =ds_dict):
    
    domo.datasets.delete(ds_dict[dataset_name])
    domo.logger.info("Deleted DataSet {}".format(ds_dict[dataset_name]))
    del(ds_dict[dataset_name])    

def update_dataset(dataset_name,
        dataset_df=None,
        csv_file_path=None,
        CLIENT_ID=CLIENT_ID,
        CLIENT_SECRET=CLIENT_SECRET,
        API_HOST=API_HOST,
        domo=domo):
    if dataset_df is None and csv_file_path is None:
        raise Exception('both dataset_df and csv_file_path cannot be None')

    if dataset_df is None:
        dataset_df = pd.read_csv(csv_file_path)
    
    if dataset_name not in ds_dict.keys():
        raise Exception('Dataset does not exist in DOMO \n run create_datasets() first')

    former_columns = [x['name'] for x in domo.datasets.get(ds_dict[dataset_name])['schema']['columns']]
    former_columns.sort()
    new_columns = list(dataset_df.columns)
    new_columns.sort()

    if len(set(new_columns))!=len(new_columns):
        raise Exception('There are duplicate column names in the dataset. Please resolve then reload')

    if not former_columns == new_columns:
        print('columns have changed in either name or size')
        print('former columns: \n',former_columns)
        print('   new columns: \n',new_columns)
        print('new columns will be used')
        change_schema(dataset_name,dataset_df,csv_file_path,CLIENT_ID,CLIENT_SECRET,API_HOST)    
    
    if csv_file_path is None:
        domo.datasets.data_import(ds_dict[dataset_name], dataset_df.to_csv(index=False))
    else:
        domo.datasets.data_import_from_file(ds_dict[dataset_name], csv_file_path)

    domo.logger.info("Uploaded data from a file to DataSet {}".format(ds_dict[dataset_name]))
    

def change_schema(dataset_name,
            dataset_df=None,
            csv_file_path=None,
            CLIENT_ID=CLIENT_ID,
            CLIENT_SECRET=CLIENT_SECRET,
            API_HOST=API_HOST,
            domo=domo,
            ds_dict =ds_dict):
    
    schema  = []

    if dataset_df is None:
        dataset_df = pd.read_csv(csv_file_path)

    try:
        dataset_df.date = pd.to_datetime(dataset_df.date)
    except:
        pass

    update = DataSetRequest()

    for col in dataset_df.columns:
        try:
            if type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==np.float64:
                WHAT_TYPE=ColumnType.DECIMAL
            elif type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==str:
                WHAT_TYPE=ColumnType.STRING
            elif type(tuple(dataset_df.iloc[np.where(list(~pd.isnull(dataset_df[col])))][col])[0])==pd.Timestamp:
                WHAT_TYPE=ColumnType.DATETIME
            else:
                WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
        except:
            WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
    
    update.schema = Schema(schema)
    updated_dataset = domo.datasets.update(ds_dict[dataset_name], update)
    domo.logger.info("Updated Schema of DataSet {}".format(ds_dict[dataset_name]))
    

def export_dataset(dataset_name,
                csv_file_path=None,
                CLIENT_ID=CLIENT_ID,
                CLIENT_SECRET=CLIENT_SECRET,
                API_HOST=API_HOST,
                domo=domo):

    if dataset_name not in ds_dict.keys():
        raise Exception('Dataset does not exist. Confirm the name you entered is correct \n Note names are case-sensitive')

    if csv_file_path is None:
        data_df = domo.datasets.data_export(ds_dict[dataset_name],include_csv_header=True)
        domo.logger.info("Downloaded data from DataSet {}".format(
            ds_dict[dataset_name]))

        data_df = data_df.split('\n')
        data_df = [x.split(',') for x in data_df]
        data_df = pd.DataFrame(data_df)
        data_df.columns = data_df.iloc[0]
        data_df.drop(0,axis=0,inplace=True)
        return(data_df)

    else:
        csv_file = domo.datasets.data_export_to_file(
                        ds_dict[dataset_name],
                        csv_file_path,
                        include_csv_header = True
                        )
        csv_file.close()
        domo.logger.info("Downloaded data as a file from DataSet {}".format(
            ds_dict[dataset_name]))
