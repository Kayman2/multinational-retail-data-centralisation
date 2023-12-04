import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
import boto3 

'''  DatabaseConnector Class '''
class DatabaseConnector:
    def __init__(self):
      pass

    ''' This method loads the yaml file from db_creds.yaml '''  
    def read_db_credentials(self):
        with open('db_creds.yaml', 'r') as file:
            configuration = yaml.safe_load(file)
        return configuration
    
    ''' This method creates engine using AWS RDS '''
    def init_db_engine(self):
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
        return engine

    ''' This method creates engine using PostgresSQL'''
    def init_PG_engine(self):
        db_username = 'postgres'
        db_password = 'Wingchun1!'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'sales_data'

        # Create a database connection
        db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
        # change to local engine
        engine = create_engine(db_url)

        #engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
        return engine

    ''' This method sends bd_creds.yaml file to SQL '''
    def upload_to_db(self, df, table_name , engine):
        df.to_sql(table_name, engine, if_exists='replace')



    def run_methods(self):
        configuration = DatabaseConnector().read_db_credentials()
        DatabaseConnector().init_db_engine
    
        print(configuration)
if __name__ == '__main__':
     DatabaseConnector().run_methods()




        

