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

def initialize(client_id=CLIENT_ID, client_secret=CLIENT_SECRET,api_host=API_HOST):
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    return Domo(CLIENT_ID, CLIENT_SECRET, logger_name='foo', log_level=logging.INFO, api_host=API_HOST)

def create_datasets(dataset_name,description,csv_file_path,CLIENT_ID=CLIENT_ID,CLIENT_SECRET=CLIENT_SECRET,API_HOST=API_HOST):
    domo = initialize(client_id=CLIENT_ID, client_secret=CLIENT_SECRET,api_host=API_HOST)
    domo.logger.info("\n**** Running a Domo API Instance ****\n")
    datasets = domo.datasets

    # Delete if exists
    existing_set = [x['id'] for x in list(domo.datasets.list()) if dataset_name == x['name']]

    if len(existing_set)!=0:
        raise Exception('Dataset already exists \n consider update_dataset()')
        # delete_dataset(domo,dataset_name)

    # Define a DataSet Schema
    dsr = DataSetRequest()
    dsr.name = dataset_name
    dsr.description = description

    dataset = pd.read_csv(csv_file_path)
    schema  = []
    for col in dataset.columns:
        try:
            if type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==np.float64:
                WHAT_TYPE=ColumnType.DECIMAL
            elif type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==str:
                WHAT_TYPE=ColumnType.STRING
            elif type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==pd.Timestamp:
                WHAT_TYPE=ColumnType.DATETIME
            else:
                WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
        except:
            WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
            
    dsr.schema= Schema(schema)
    dataset = datasets.create(dsr)
    # update_dataset(dataset_name,csv_file_path)
    domo.logger.info("Created DataSet " + dataset['id'])
    del(domo)
    update_dataset(dataset_name,csv_file_path)

def delete_dataset(dataset_name,CLIENT_ID=CLIENT_ID,CLIENT_SECRET=CLIENT_SECRET,API_HOST=API_HOST):
    domo = initialize(CLIENT_ID,CLIENT_SECRET,API_HOST)
    data_id = [x['id'] for x in list(domo.datasets.list()) if dataset_name == x['name']][0]
    domo.datasets.delete(data_id)
    domo.logger.info("Deleted DataSet {}".format(data_id))
    del(domo)

def update_dataset(dataset_name,csv_file_path,CLIENT_ID=CLIENT_ID,CLIENT_SECRET=CLIENT_SECRET,API_HOST=API_HOST):
    domo = initialize(CLIENT_ID,CLIENT_SECRET,API_HOST)
    csv_file_path = csv_file_path
    dataset_df = pd.read_csv(csv_file_path)

    try:
        data_id = [x['id'] for x in list(domo.datasets.list()) if dataset_name == x['name']][0]
    except:
        raise Exception('Dataset does not exist in DOMO \n run create_datasets() first')

    former_columns = [x['name'] for x in domo.datasets.get(data_id)['schema']['columns']]
    former_columns.sort()
    new_columns = dataset_df.columns.values
    new_columns.sort()

    if not (former_columns == new_columns).all():
        print('columns have changed in either name or size')
        print('former columns: \n',former_columns)
        print('   new columns: \n',new_columns)
        print('new columns will be used')
        change_schema(dataset_name,csv_file_path,CLIENT_ID,CLIENT_SECRET,API_HOST)    
    
    domo.datasets.data_import_from_file(data_id, csv_file_path)
    domo.logger.info("Uploaded data from a file to DataSet {}".format(data_id))
    del(domo)

def change_schema(dataset_name,new_csv_path,CLIENT_ID=CLIENT_ID,CLIENT_SECRET=CLIENT_SECRET,API_HOST=API_HOST):
    domo = initialize(CLIENT_ID,CLIENT_SECRET,API_HOST)
    data_df = pd.read_csv(new_csv_path)
    data_id = [x['id'] for x in list(domo.datasets.list()) if dataset_name == x['name']][0]
    schema  = []

    data_df = pd.read_csv(new_csv_path)
    try:
        data_df.date = pd.to_datetime(data_df.date)
    except:
        pass

    dataset = data_df

    update = DataSetRequest()

    for col in dataset.columns:
        try:
            if type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==np.float64:
                WHAT_TYPE=ColumnType.DECIMAL
            elif type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==str:
                WHAT_TYPE=ColumnType.STRING
            elif type(tuple(dataset.iloc[np.where(list(~pd.isnull(dataset[col])))][col])[0])==pd.Timestamp:
                WHAT_TYPE=ColumnType.DATETIME
            else:
                WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
        except:
            WHAT_TYPE=ColumnType.DECIMAL
            schema.append(Column(WHAT_TYPE,col))
    
    update.schema = Schema(schema)
    updated_dataset = domo.datasets.update(data_id, update)
    domo.logger.info("Updated Schema of DataSet {}".format(data_id))
    del(domo)




