
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


"""
This Python file is responsible for data cleaning operations. It utilizes various data sources such as RDS tables,
PDFs, APIs, and S3 buckets. The data is extracted, cleaned, and standardized before being uploaded to a specified
database. The main focus is on cleaning user data, product weights, card data, store data, and sales dates.
"""


class DataCleaning:
    """
    The DataCleaning class is responsible for cleaning and preparing data for database upload.

    Attributes:
        df_orders (DataFrame): A pandas DataFrame containing order data.
        df_pdf (DataFrame): A pandas DataFrame containing data from PDF files.
        df_api (DataFrame): A pandas DataFrame containing data from API responses.
        df_bucket (DataFrame): A pandas DataFrame containing data from S3 buckets.
        sales_date_df (DataFrame): A pandas DataFrame containing sales date data.
    """

    def __init__(self, engine=DatabaseConnector().init_db_engine(), table_name='legacy_users'):
        self.df_orders = DataExtractor(engine).read_rds_tables(table_name)
        self.df_pdf = DataExtractor(engine).retrieve_pdf_data()
        self.df_api = DataExtractor(engine).retrieve_stores_data(num_of_stores=451)
        self.df_bucket = DataExtractor(engine).extract_from_s3()
        self.sales_date_df = DataExtractor(engine).sales_date()

    ''' 
	drops Null values in df_orders 
    

    def clean_user_data(self):
        self.df_orders = self.df_orders.dropna(how='any').dropna(how='any', axis=1)
        self.df_orders.update(self.df_orders)
        self.df_orders = self.df_orders.reset_index(drop=True)
        self.df_orders.update(self.df_orders)
        return self.df_orders
'''

    def standardise_user_data(self):
        """
        Standardizes user data by cleaning and formatting the phone_number column and filtering
        by allowed country codes.

        Returns:
            DataFrame: The standardized orders DataFrame.
        """
        self.df_orders['phone_number'] = self.df_orders['phone_number'].astype('str')
        self.df_orders['phone_number'] = self.df_orders['phone_number'].str.replace(r'[^0-9]+', '')

        self.df_orders = self.df_orders.dropna(how='any').dropna(how='any', axis=1)

        self.df_orders['country_code'] = self.df_orders['country_code'].str.slice(-2)

        # List of allowed country codes
        allowed_country_codes = ['GB', 'US', 'DE']
        # Use boolean indexing to filter rows
        self.df_orders = self.df_orders[self.df_orders['country_code'].isin(allowed_country_codes)]

        # Convert the text column to a datetime format
        self.df_orders['date_of_birth'] = self.df_orders['date_of_birth'].apply(self.convert_to_iso8601)

        self.df_orders['join_date'] = self.df_orders['join_date'].apply(self.convert_to_iso8601)

        return self.df_orders

    def convert_to_iso8601(self, date_str):
        """
        Converts a date string to ISO 8601 format (YYYY-MM-DD).

        Parameters:
            date_str (str): The date string to be converted.

        Returns:
            str: The converted date string in ISO 8601 format or None if conversion is not possible.
        """
        # Check if date_str is not a string
        if not isinstance(date_str, str):
            return None

        try:
            # Try parsing as ISO 8601 format
            parsed_date = pd.to_datetime(date_str)
            # Check if the parsed date is NaT
            if pd.isna(parsed_date):
                raise ValueError
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            # Handle other date formats here
            parts = date_str.split()
            if len(parts) == 3:
                try:
                    # Try parsing formats like "2018 October 22"
                    if parts[0].isdigit():
                        year = parts[0]
                        month = pd.to_datetime(parts[1], format='%B').month
                        day = parts[2]
                    # Try parsing formats like "October 2018 22"
                    else:
                        month = pd.to_datetime(parts[0], format='%B').month
                        year = parts[1]
                        day = parts[2]
                    return f"{year}-{month:02d}-{day}"
                except ValueError:
                    return None  # Return None for unhandled formats
            else:
                return None  # Return None for unhandled formats

    def clean_card_data(self):
            """
            Cleans the card data in the DataFrame.

            This method performs multiple cleaning steps on card data:
            - Replaces any '?' characters with an empty string in the 'card_number' column.
            - Filters the rows in the 'expiry_date' column to retain only those matching the 'mm/dd' format.
            - Converts the 'date_payment_confirmed' column to a datetime format in ISO 8601.

            The cleaning is done on the DataFrame stored in the `df_pdf` attribute of the class.

            Returns:
                DataFrame: A cleaned DataFrame with the updated card data.
            """

            # Replace '?' with an empty string in 'card_number' column
            self.df_pdf['card_number'] = self.df_pdf['card_number'].str.replace('?', '', regex=False)

            # Define a regular expression pattern for the "mm/dd" format
            pattern = r'\d{2}/\d{2}'

            # Create a boolean mask to filter rows that match the pattern
            mask = self.df_pdf['expiry_date'].str.contains(pattern, na=False)

            # Use the mask to filter rows and update self.df_pdf
            self.df_pdf = self.df_pdf[mask]

            # Convert the text column to a datetime format
            self.df_pdf['date_payment_confirmed'] = self.df_pdf['date_payment_confirmed'].apply(self.convert_to_iso8601)

            return self.df_pdf



    def called_clean_store_data(self):
        """
        Cleans the store data in the DataFrame.

        This method performs various cleaning operations on the store data:
        - Filters rows to only include those with country codes in a specified list (GB, US, DE).
        - Removes non-numeric characters from the 'staff_numbers' column.
        - Removes the characters 'ee' from the 'continent' column.
        - Replaces 'N/A' with '0.0' in both 'latitude' and 'longitude' columns and filters out rows where these values are '0.0'.
        - Converts the 'opening_date' column to a datetime format in ISO 8601.

        These operations are performed on the DataFrame stored in the `df_api` attribute of the class.

        Returns:
            DataFrame: A cleaned DataFrame with the updated store data.
        """

        # Check if 'country_code' column exists
        if 'country_code' in self.df_api.columns:
            allowed_country_codes = ['GB', 'US', 'DE']
            self.df_api = self.df_api[self.df_api['country_code'].isin(allowed_country_codes)]
        else:
            print("'country_code' column not found in DataFrame")

        # Remove alphabetic characters from the 'staff_numbers' column
        self.df_api['staff_numbers'] = self.df_api['staff_numbers'].str.replace(r'[^0-9]', '', regex=True)
        # Remove ee alphabetic characters from the 'continent' column
        self.df_api['continent'] = self.df_api['continent'].str.replace('ee', '')

        # Replace N/A with a specific default value (e.g., 0.0)
        # Replace 'N/A' with ').)' in the 'Latitude' column
        #self.df_api['latitude'] = self.df_api['latitude'].replace('N/A', '0.0')

        # Now, filtering the dataframe to show rows where Latitude is ').)'
        #self.df_api['latitude'] = self.df_api[self.df_api['latitude'] == '0.0']



        #self.df_api['longitude'] = self.df_api['longitude'].replace('N/A', '0.0')
        # Now, filtering the dataframe to show rows where Latitude is ').)'
        #self.df_api['longitude'] = self.df_api[self.df_api['longitude'] == '0.0']


         self.df_api['longitude'] = self.df_api['longitude'].apply(lambda x: 0.0 if x == 'N/A' else x)
        #self.df_api['latitude'] = self.df_api['latitude'].apply(lambda x: 0.0 if x == 'N/A' else x)


        # Convert the text column to a datetime format
        self.df_api['opening_date'] = self.df_api['opening_date'].apply(self.convert_to_iso8601)
        return self.df_api



    def convert_product_weights(self):
        """
        Converts product weight data to a uniform format in kilograms.

        This method executes the following steps on the product weight data:
        1. Extracts numerical weight values from the 'weight' column and converts them to floats.
        2. Handles compound weight expressions (e.g., '2x3') by evaluating the expression to calculate the total weight.
        3. Converts weights from grams to kilograms if they are not already in kilograms.
        4. Removes the original 'weight' column and renames the 'weights_in_kg' column to 'weights'.

        The conversions and calculations are done on the DataFrame stored in the `df_bucket` attribute of the class.

        Returns:
            DataFrame: The DataFrame with weights converted to kilograms and updated.
        """


        # Step 1: Extract numerical weight values and convert to float
        self.df_bucket['weights_in_kg'] = self.df_bucket['weight'].str.extract(r'(\d+\.\d+|\d+)').astype('float')

        # Step 2: Handle compound weight expressions (like '2x3')
        # Using apply with a lambda function for better readability and direct assignment
        def evaluate_expression(weight):
            if 'x' in str(weight):
                weight = weight.replace('x', '*').replace(' ', '')
                return eval(weight)
            return weight

        self.df_bucket['weights_in_kg'] = self.df_bucket['weights_in_kg'].apply(evaluate_expression)

        # Step 3: Convert weights from grams to kilograms if needed
        # Identify cells with weights not in kilograms
        not_in_kg = self.df_bucket['weight'].str.contains('g', na=False) & ~self.df_bucket['weight'].str.contains('kg',
                                                                                                                  na=False)

        # Convert these weights from grams to kilograms
        self.df_bucket.loc[not_in_kg, 'weights_in_kg'] /= 1000

        # Delete the 'weight' column
        self.df_bucket.drop(columns=['weight'], inplace=True)

        # Rename 'weights_in_kg' column to 'weight'
        self.df_bucket.rename(columns={'weights_in_kg': 'weights'}, inplace=True)

        return self.df_bucket


    def clean_products_data(self):
        """
        Cleans the product data in the DataFrame.

        This method carries out several data cleaning operations on the product data:
        - Drops rows where all elements are NaN.
        - Converts the 'removed' and 'category' columns to categorical data types.
        - Filters out rows where the 'product_price' column exceeds 7 characters in length.
        - Converts the 'date_added' column to a datetime format in ISO 8601.

        These operations are performed on the DataFrame stored in the `df_bucket` attribute of the class.

        Returns:
            DataFrame: The cleaned DataFrame with updated product data.
        """
        self.df_bucket = self.df_bucket.dropna(how='all')
        self.df_bucket['removed'] = self.df_bucket['removed'].astype('category')
        self.df_bucket['category'] = self.df_bucket['category'].astype('category')

        # Create a boolean mask for rows with 'product_price' > 7 characters
        mask = self.df_bucket['product_price'].str.len() > 7
        # Use the mask to filter the DataFrame and keep only the rows that meet the condition
        self.df_bucket = self.df_bucket[~mask]
        # Convert the text column to a datetime format
        self.df_bucket['date_added'] = self.df_bucket['date_added'].apply(self.convert_to_iso8601)
        return self.df_bucket



    def clean_orders_data(self):
        """
        Cleans the orders data from a specified database table.

        This method performs the following operations:
        - Initializes a database connection and extracts data from the 'orders_table'.
        - Drops specified columns ('first_name', 'last_name', '1', 'index') from the DataFrame.
        - Removes rows with any NaN values.

        The method uses the `DatabaseConnector` and `DataExtractor` utilities to interact with the database.

        Returns:
            DataFrame: The cleaned orders DataFrame.
        """
        table_name = 'orders_table'
        engine = DatabaseConnector.init_db_engine(self)
        self.new_df_orders = DataExtractor(engine).read_rds_tables(table_name)
        self.new_df_orders = self.new_df_orders.drop(["first_name", "last_name", "1", "index"], axis=1)
        self.new_df_orders = self.new_df_orders.dropna(how='any').dropna(how='any', axis=1)
        return self.new_df_orders



    def cleaning_sales_date(self):
        """
        Cleans the sales date data in the DataFrame.

        This method performs several data cleaning operations on the sales date data:
        - Converts the 'time_period' column to a categorical data type.
        - Drops rows with any NaN values in the DataFrame.
        - Filters rows based on the condition that the length of the 'month' column should be 2 characters or less.
        - Resets the index of the DataFrame.

        These operations are applied to the DataFrame stored in the `sales_date_df` attribute of the class.

        Returns:
            DataFrame: The cleaned sales date DataFrame with updated data.
        """

        self.sales_date_df['time_period'] = self.sales_date_df['time_period'].astype('category')
        self.sales_date_df = self.sales_date_df.dropna(how='any').dropna(how='any', axis=1)
        # Filter rows based on the condition (length of 'month' column <= 2)
        self.sales_date_df = self.sales_date_df[self.sales_date_df['month'].str.len() <= 2]

        # Reset the index if needed
        self.sales_date_df.reset_index(drop=True, inplace=True)

        return self.sales_date_df


    def then_upload_to_db(self):
        """
        Cleans various datasets and uploads them to the respective tables in the database.

        The method performs the following operations:
        1. Cleans the products data and uploads it to the 'dim_products' table.
        2. Cleans the store data and uploads it to the 'dim_store_details' table.
        3. Cleans the sales date data and uploads it to the 'dim_date_times' table.
        4. Cleans the card data and uploads it to the 'dim_card_details' table.
        5. Cleans the orders data after standardization and uploads it to the 'dim_users' table.
        6. Cleans the orders data again and uploads it to the 'orders_table' table.

        Each step involves cleaning the data using specific methods from the class and then uploading
        the cleaned data to a designated table in the database using the DatabaseConnector utility.


        """
        engine = DatabaseConnector().init_PG_engine()

        # 1. dim_products: Clean products data and upload to the database
        df_bucket = self.clean_products_data()
        df_bucket_weight = self.convert_product_weights()
        DatabaseConnector().upload_to_db(df_bucket, 'dim_products', engine)
        DatabaseConnector().upload_to_db(df_bucket_weight, 'dim_products', engine)

        # 2. dim_store_details: Clean store data and upload to the database
        df_api = self.called_clean_store_data()
        DatabaseConnector().upload_to_db(df_api, 'dim_store_details', engine)

        # 3. dim_date_times : Clean sales date data and upload to the database
        sales_date_df = self.cleaning_sales_date()
        DatabaseConnector().upload_to_db(sales_date_df, 'dim_date_times', engine)

        # 4. dim_card_details : Clean card data and upload to the database
        df_pdf = clean_data.clean_card_data()
        DatabaseConnector().upload_to_db(df_pdf, 'dim_card_details', engine)

        # 5.  dim_users : Clean orders and upload to dim_users table
        df_orders = clean_data.standardise_user_data()
        DatabaseConnector().upload_to_db(df_orders, 'dim_users', engine)

        # 6. orders_table : Clean orders table uploads to orders_table table
        df_new_orders = clean_data.clean_orders_data()
        DatabaseConnector().upload_to_db(df_new_orders, 'orders_table', engine)


if __name__ == '__main__':
    clean_data = DataCleaning(engine=DatabaseConnector().init_db_engine())
    clean_data.then_upload_to_db()
