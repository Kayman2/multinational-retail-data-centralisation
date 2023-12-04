import requests
from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from tabula import read_pdf
import tabula
import numpy as np
import psycopg2
import yaml
import pandas as pd
import requests
import json
import time
import boto3 
import io
import os
from botocore import UNSIGNED
from botocore.client import Config


''' This Class extracts data '''
class DataExtractor: 

   def __init__(self, engine):
     self.engine = engine

   ''' This method lists all tables '''
   def list_db_tables(self):
     inspector = inspect(self.engine)
     return inspector.get_table_names() 
   
   '''  This method reads the tables '''
   def read_rds_tables(self,table_name):
      table = pd.read_sql_table(table_name, self.engine)
      return table
   
   ''' This method retrieves pdf_data from AWS s3 bucket '''    
   def retrieve_pdf_data(self):
      url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
      pdf_list = tabula.read_pdf(url, pages='all')
      pdf_to_df = pd.concat(pdf_list, ignore_index=True)
      return pdf_to_df
   
   ''' This method lists the number of stores from API '''   
   def list_number_of_stores(self):
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
      num_of_stores = requests.get(api_url, headers = header_dict).json()
      return num_of_stores
   
   ''' This method retrieves the listed number of stores gotten from the API '''   
   def retrieve_stores_data(self, num_of_stores):
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      retrieve_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
      store_data=[]  
      for page in range (num_of_stores):
         ext_all_stores = requests.get(retrieve_store_url + str(page),headers=header_dict).json()
         column_heading = ext_all_stores.keys()
         store_data.append(list(ext_all_stores.values()))  
      df_api = pd.DataFrame((store_data),columns=column_heading)
      return df_api
   
   ''' This method extracts products_data from AWS S3 '''   
   def extract_from_s3(self):
      s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
      products_data = s3.get_object(Bucket='data-handling-public', Key='products.csv')
      products_df = pd.read_csv(products_data['Body'])
      return products_df
   
   ''' This method gets a json file from AWS s3 '''   
   def sales_date(self):
      date_data_url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
      date_sales = requests.get(date_data_url).json()
      sales_date_df = pd.DataFrame(date_sales) 
      return sales_date_df
   
   ''' This method calls and instantiate all methods '''
   def run_code(self):
      engine_obj = DatabaseConnector()
      engine = engine_obj.init_db_engine() 
      data_ext = DataExtractor(engine) 
      data_ext.list_db_tables()
      table_name = 'legacy_users' 
      data_ext.read_rds_tables(table_name) 
      df_pdf = data_ext.retrieve_pdf_data()
      num_of_stores = data_ext.list_number_of_stores()
      num_of_stores = 451
    #  df_api = data_ext.retrieve_stores_data(num_of_stores)
      data_ext.extract_from_s3()
      sales_date_df = data_ext.sales_date()
      
     
if __name__ == '__main__':
 engine_obj = DatabaseConnector()
 engine = engine_obj.init_db_engine()
 DataExtractor(engine).run_code()

    
   






   