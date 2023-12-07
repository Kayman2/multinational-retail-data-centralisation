from sqlalchemy import inspect
from database_utils import DatabaseConnector
import tabula
import pandas as pd
import requests
import boto3
from botocore import UNSIGNED
from botocore.client import Config


class DataExtractor:
   """
   Class for extracting data from various sources including a relational database,
   PDF files from a URL, data from an API, and files from AWS S3.

   Attributes:
       engine (Engine): A SQLAlchemy Engine object for database connection.
   """

   def __init__(self, engine):
      """
      Initializes the DataExtractor with a database engine.

      Args:
          engine (Engine): A SQLAlchemy Engine object for database connection.
      """
      self.engine = engine

   def list_db_tables(self):
      """
      Lists all tables in the database connected to the provided engine.

      Returns:
          list: A list of table names in the database.
      """
      inspector = inspect(self.engine)
      return inspector.get_table_names()

   def read_rds_tables(self, table_name):
      """
      Reads a table from the connected relational database system (RDS) into a Pandas DataFrame.

      Args:
          table_name (str): The name of the table to read.

      Returns:
          DataFrame: A Pandas DataFrame containing the data from the specified table.
      """
      table = pd.read_sql_table(table_name, self.engine)
      return table

   def retrieve_pdf_data(self):
      """
      Retrieves data from a PDF file located at a specified URL and converts it to a Pandas DataFrame.

      Returns:
          DataFrame: A DataFrame containing data extracted from the PDF.
      """
      url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
      pdf_list = tabula.read_pdf(url, pages='all')
      pdf_to_df = pd.concat(pdf_list, ignore_index=True)
      return pdf_to_df

   def list_number_of_stores(self):
      """
      Retrieves the number of stores from a specified API endpoint.

      Returns:
          dict: A dictionary containing the number of stores.
      """
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
      num_of_stores = requests.get(api_url, headers=header_dict).json()
      return num_of_stores

   def retrieve_stores_data(self, num_of_stores):
      """
      Retrieves store data from a specified API endpoint.

      Args:
          num_of_stores (int): The number of stores to retrieve data for.

      Returns:
          DataFrame: A DataFrame containing store data.
      """
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      retrieve_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
      store_data = []
      for page in range(num_of_stores):
         ext_all_stores = requests.get(retrieve_store_url + str(page), headers=header_dict).json()
         column_heading = ext_all_stores.keys()
         store_data.append(list(ext_all_stores.values()))
      df_api = pd.DataFrame(store_data, columns=column_heading)
      return df_api

   def extract_from_s3(self):
      """
      Extracts product data from an AWS S3 bucket and loads it into a Pandas DataFrame.

      Returns:
          DataFrame: A DataFrame containing product data.
      """
      s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
      products_data = s3.get_object(Bucket='data-handling-public', Key='products.csv')
      products_df = pd.read_csv(products_data['Body'])
      return products_df

   def sales_date(self):
      """
      Retrieves sales date data from a JSON file hosted on AWS S3 and converts it to a DataFrame.

      Returns:
          DataFrame: A DataFrame containing sales date data.
      """
      date_data_url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
      date_sales = requests.get(date_data_url).json()
      sales_date_df = pd.DataFrame(date_sales)
      return sales_date_df

   def run_code(self):
      # List all tables in the database
      tables = self.list_db_tables()
      print("Tables in the database:", tables)

      # Read a specific table from the database
      table_name = 'legacy_users'
      legacy_users_df = self.read_rds_tables(table_name)
      print("Legacy Users Data:", legacy_users_df.head())

      # Retrieve PDF data
      df_pdf = self.retrieve_pdf_data()
      print("PDF Data:", df_pdf.head())

      # Get the number of stores from API
      num_of_stores = self.list_number_of_stores()
      print("Number of Stores:", num_of_stores)

      # Retrieve store data
      #df_api = self.retrieve_stores_data(num_of_stores)
      #print("Store Data:", df_api.head())

      # Extract products data from S3
      products_df = self.extract_from_s3()
      print("Products Data:", products_df.head())

      # Get sales date data
      sales_date_df = self.sales_date()
      print("Sales Date Data:", sales_date_df.head())


if __name__ == '__main__':
 engine_obj = DatabaseConnector()
 engine = engine_obj.init_db_engine()
 DataExtractor(engine).run_code()

    
   






   