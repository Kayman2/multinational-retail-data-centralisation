# Multinational-Retail-Data-Centralisation
Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. By utilising the power of Pandas, the project will clean the data, and produce a STAR based database schema for optimised data storage and access. The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. This project will provide the user with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 
* Developed a system that extracts retail sales data from five different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
* Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
* Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
* Used complex SQL queries to derive insights and to help reduce costs by 15%
* Queried the data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales. 

 
1: Environment setup

* GitHub repo created


2: Extracted and cleaned the data from data sources

* Setting up a new database on pgAdmin4 named Sales_Data

* New python script was created named data_extraction.py and within it, a class DataExtractor was created to extract csv files, an API and S3 bucket files

* Created a python script named database_utils.py and within it, a class DatabaseConnector was created to connect and upload data to the database

* Created another script named data_cleaning and within it, class DataCleaning was created with methods to clean data from each of the data source

* Created db_cred.yaml file with given credentials inside DatabaseConnector then developed a method to extract the data from the database

* init_bd_engine, read_rds_table were created

* Uploaded to sales_data database using upload_to_db method called dim_users table

* Created retrieve_pdf_data in DataExtractor class which uses tabula to extract the pages from a given link

* Created clean_card_data inside DataCleaning class and uploaded to a table called dim_card_details

* Extracted data therough an API with given key and value

*  Created a method called list_number_of_stores inside class DataExtractor which returns the number of stores to extract

* Created another method called retrieve_store_data which takes the retrieved store endpoint as an argument and extracts all the stores from the API, saving them in a pandas Dataframe

* Another method was created called called_clean_store_data in DataCleaning class, which cleanse the retrieved data from the API and returns a pandas Dataframe

* Using upload_to_db method to send to database with table name dim_store_details

* Extract and clean the product details

* Extracting a csv format file from s3 bucket on AWS. Created a method in DataExtractor named extract_from_s3 to extract from a given s3 address. 

* Created a method named convert_product_weights to convert weights in all other values to kg.

* Another method was created called clean_products_data which cleanse the returned dataframe and uploaded it to the database using upload_to_db method with table name dim_products

* Using  list_db_tables method, extracted the orders data using read_rds_table1 and returned a pandas dataframe

* Created  a new method called clean_orders_data which cleanse  and removed some columns and then uploaded to the database using upload_to_db with table name orders_table

* The final cource of data is a json file with a given link stored on AWS s3. Cleanse and stored in the database with table name dim_date_times


3: Creating a database schema (SQL)

* Casted the columns of orders_table to the correct data types

* Casted the columns of dim_users_table to the correct data types

* Casted the columns of dim_store_details to the correct data types

* In dim_products table, created a new columns called weight range in kg 

* Casted the columns of dim_products to the correct data types and renaming the column "removed" as "still_available"

* Casted the columns of dim_date_times to the correct data types

* Casted the columns of dim_card_details to the correct data types

* Created the primary keys in the dimension tables

* Adding the foreign key to orders_table


4: Querying the data

* Performing queries on how many stores does the business have and in which countries?

* Which location currently have the most stores?

* Which month produce the average highest cost of sales typically?

* How many sales are coming from online?

* What percentage of sales comes through each type of store?

* Which month in each year produced the highest cost of sales?

* What is the staff headcount?

* Which German store type is selling the most?

* How quickly is the company making sales?
     * To run: python data_cleaning.py


