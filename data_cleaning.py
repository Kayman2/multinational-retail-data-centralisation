import psycopg2
import yaml
import pandas as pd
import numpy as np
import re
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from sqlalchemy import create_engine

''' 
	cleans all the extracted files 
'''


class DataCleaning:
    def __init__(self, engine=DatabaseConnector().init_db_engine(), table_name='legacy_users'):
        self.df_orders = DataExtractor(engine).read_rds_tables(table_name)
        self.df_pdf = DataExtractor(engine).retrieve_pdf_data()
        self.df_api = DataExtractor(engine).retrieve_stores_data(num_of_stores=451)
        self.df_bucket = DataExtractor(engine).extract_from_s3()
        self.sales_date_df = DataExtractor(engine).sales_date()


    ''' 
	drops Null values in df_orders 
    '''

    def clean_user_data(self):
        self.df_orders = self.df_orders.dropna(how='any').dropna(how='any', axis=1)
        self.df_orders.update(self.df_orders)
        self.df_orders = self.df_orders.reset_index(drop=True)
        self.df_orders.update(self.df_orders)
        return self.df_orders


    ''' 
	cleans and formats the phone_number column 
    '''

    def standardise_phone_number(self):
        self.df_orders['phone_number'] = self.df_orders['phone_number'].astype('str')
        self.df_orders['phone_number'] = self.df_orders['phone_number'].str.replace(r'[^0-9]+', '')
        self.df_orders.update(self.df_orders)
        return self.df_orders

    
    ''' 
	concat df_pdf, replaces special characters,  removes white spaces 
    '''

    def clean_card_data(self):
        df_pdf = self.df_pdf
        df_pdf = df_pdf.replace('?', '')
        df_pdf.update(df_pdf)
        df_pdf = df_pdf.dropna(how='any').dropna(how='any', axis=1)
        df_pdf.update(df_pdf)
        return df_pdf


    ''' 
	cleans df_api file 
    '''

    def called_clean_store_data(self):
        self.df_api.drop_duplicates()
        self.df_api = self.df_api.dropna(how='all')
        self.df_api = self.df_api.dropna(axis=1)
        self.df_api.update(self.df_api)
        return self.df_api


    ''' 
	creates a new column weights_in_kg, replaced x with *, 
        removed white space and evaluate the records 
    '''

    def convert_product_weights(self):
        self.df_bucket['weights_in_kg'] = self.df_bucket['weight'].str.extract(r'(\d+.\d+)').astype('float')
        for page in self.df_bucket['weights_in_kg']:
            if 'x' in str(page):
                page = page.replace('x', '*')
                page = page.replace(' ', '')
                page = eval(page)

        cells_to_divide = self.df_bucket['weight'].str.contains('kg', na=False)
        self.df_bucket['weights_in_kg'].iloc[~cells_to_divide.values] = self.df_bucket['weights_in_kg'].iloc[
            ~cells_to_divide.values].multiply(0.001)
        return self.df_bucket

   
    ''' 
	cleans the df_bucket 
  
    '''

    def clean_products_data(self):
        self.df_bucket = self.df_bucket.dropna(how='all')
        self.df_bucket['removed'] = self.df_bucket['removed'].astype('category')
        self.df_bucket['category'] = self.df_bucket['category'].astype('category')
        return self.df_bucket

    ''' 
	removes some columns and dropped NaN values 
    
    '''

    def clean_orders_data(self):
        table_name = 'orders_table'
        engine = DatabaseConnector.init_db_engine(self)
        self.new_df_orders = DataExtractor(engine).read_rds_tables(table_name)
        self.new_df_orders = self.new_df_orders.drop(["first_name", "last_name", "1", "index"], axis=1)
        self.new_df_orders = self.new_df_orders.dropna(how='any').dropna(how='any', axis=1)
        return self.new_df_orders

 
   ''' 
	cleans sales_date_df file 
   
   '''

    def cleaning_sales_date(self):
        self.sales_date_df['time_period'] = self.sales_date_df['time_period'].astype('category')
        self.sales_date_df = self.sales_date_df.dropna(how='any').dropna(how='any', axis=1)
        self.sales_date_df = self.sales_date_df.drop_duplicates()
        self.sales_date_df.update(self.sales_date_df)
        return self.sales_date_df



    ''' 
	calls and instantiates the methods 

    '''

    def test_cleaning(self):
        # Database connection parameters
        db_username = 'postgres'
        db_password = 'Wingchun1!'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'sales_data'

        # Create a database connection
        db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
        #change to local engine
        engine = create_engine(db_url)


# WORKS FINE!!!
#        clean_data = DataCleaning()
#        df_pdf = clean_data.clean_card_data()
#        DatabaseConnector().upload_to_db(df_pdf, 'dim_card_details', engine)

# WORKS FINE!!!
#        clean_data = DataCleaning()
#        df_api = clean_data.called_clean_store_data()
#        DatabaseConnector().upload_to_db(df_api,'dim_store_details', engine)



# WORKS FINE!!!
#        clean_data = DataCleaning()
#        df_orders = clean_data.standardise_phone_number()
#        DatabaseConnector().upload_to_db(df_orders, 'dim_users', engine)



# WORKS FINE!!!
#        clean_data = DataCleaning()
#        df_bucket = clean_data.clean_products_data()
#        DatabaseConnector().upload_to_db(df_bucket,'dim_products', engine)


# WORKS FINE!!!
#        clean_data = DataCleaning()
#        df_new_orders = clean_data.clean_orders_data()
#        DatabaseConnector().upload_to_db(df_new_orders, 'orders_table', engine)



# WORKS FINE!!!
# 	clean_data = DataCleaning()
# 	sales_date_df = clean_data.cleaning_sales_date()
# 	DatabaseConnector().upload_to_db(sales_date_df,'dim_date_times', engine)


if __name__ == '__main__':
    clean_data = DataCleaning(engine=DatabaseConnector().init_db_engine())
    clean_data.test_cleaning()
