# Multinational-Retail-Data-Centralisation
Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. 
By utilising the power of Pandas, the project will clean the data, and produce a STAR based database schema for optimised data storage and access. 
The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. 
This project will provide the user with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 

* Developed a system that extracts retail sales data from five different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
* Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
* Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
* Used complex SQL queries to derive insights and to help reduce costs by 15%
* Query data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales. 

 
1: Environment setup

		* GitHub repo created


2: Extracted and cleaned the data from data sources

		* Setting up a new database on pgAdmin4 named Sales_Data


		* Data_extraction.py : a class DataExtractor was created to extract csv files, an API and S3 bucket files
			
			* list_number_of_stores : inside class DataExtractor which returns the number of stores to extract

			* retrieve_pdf_data : DataExtractor class which uses tabula to extract the pages from a given link

			* Extracted data therough an API with given key and value

			* retrieve_store_data : which takes the retrieved store endpoint as an argument and extracts all the stores from the API, saving them in a pandas Dataframe

			* Extract and clean the product details

			* extract_from_s3 :  Extracting a csv format file from s3 bucket on AWS. Created a method in DataExtractor named extract_from_s3 to extract from a given s3 address. 

			* sales_date :  data is a json file with a given link stored on AWS s3. Cleaned and stored in the database with table name dim_date_times

		* Database_utils.py : a class DatabaseConnector was created to connect and upload data to the database

			* db_cred.yaml : a file with given credentials inside DatabaseConnector then developed a method to extract the data from the database

			* init_bd_engine : read_rds_table were created

			* upload_to_db : Uploaded to sales_data database using  method called dim_users table

			* upload_to_db : method used to send to database with table name dim_store_details

			* list_db_tables method, extracts the orders data using read_rds_table1 and returns a pandas dataframe


		* Data_Cleaning.py : a  class DataCleaning was created with methods to clean data from each of the data source

			* clean_card_data : cleans card details and uploads to a table called dim_card_details

			* clean_store_data : cleans the retrieved data from the API and returns a pandas Dataframe

			* clean_products_data : cleans the returned dataframe and uploaded it to the database using upload_to_db method with table name dim_products

			* clean_orders_data : cleans  and removed some columns and then uploaded to the database using upload_to_db with table name orders_table

  			* convert_product_weights :  to convert weights in all other values to kg.


















3: Creating a database schema (SQL)  ##TODO

	* Casted the columns of orders_table to the correct data types

	* Casted the columns of dim_users_table to the correct data types

	* Casted the columns of dim_store_details to the correct data types

	* In dim_products table, created a new columns called weight range in kg 

	* Casted the columns of dim_products to the correct data types and renaming the column "removed" as "still_available"

	* Casted the columns of dim_date_times to the correct data types

	* Casted the columns of dim_card_details to the correct data types

	* Created the primary keys in the dimension tables

	* Adding the foreign key to orders_table



4: Querying the data (SQL)    ##TODO

	TASK 1: How many stores does the business have and in which countries?

	TASK 2: Which locations currently have the most stores?

	TASK 3: Which month produced the largest amount of Sales?

	TASK 4: How many sales are coming from online?

	TASK 5: What percentage of Sales come through each type of store?

	TASK 6: Which month in each year produced the highest cost of Sales?

	TASK 7: What is our staff headcount?

	TASK 8: Which German store type is selling the most?

	TASK 9: How quickly is the company making Sales?

	 TASK 10: Update the latest code changes to Github.



