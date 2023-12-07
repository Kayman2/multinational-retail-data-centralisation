import yaml
from sqlalchemy import create_engine


class DatabaseConnector:
    """
    A class used to connect to a database using credentials from a YAML file
    and create database engines for different environments.
    """

    def __init__(self):
        """
        Initializes the DatabaseConnector instance.
        """
        pass

    def read_db_credentials(self):
        """
        Reads database credentials from a YAML file.

        Returns:
            dict: A dictionary containing database configuration.
        """
        with open('db_creds.yaml', 'r') as file:
            configuration = yaml.safe_load(file)
        return configuration

    def init_db_engine(self):
        """
        Creates and returns a SQLAlchemy engine for AWS RDS PostgreSQL database.

        Returns:
            Engine: A SQLAlchemy engine object for the AWS RDS database.
        """
        db_url = "postgresql+psycopg2://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"
        engine = create_engine(db_url)
        return engine

    def init_PG_engine(self):
        """
        Creates and returns a SQLAlchemy engine for a local PostgreSQL database.

        Returns:
            Engine: A SQLAlchemy engine object for the local PostgreSQL database.
        """
        db_username = 'postgres'
        db_password = 'Wingchun1!'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'sales_data'

        db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(db_url)

        return engine

    def upload_to_db(self, df, table_name, engine):
        """
        Uploads a DataFrame to a SQL database.

        Args:
            df (DataFrame): The DataFrame to upload.
            table_name (str): The name of the target SQL table.
            engine (Engine): The SQLAlchemy engine to use for connection.

        Returns:
            None
        """
        df.to_sql(table_name, engine, if_exists='replace')

    def test_methods(self):
        """
        Tests the class methods by reading database credentials and initializing database engine.

        Returns:
            None
        """
        configuration = self.read_db_credentials()
        self.init_db_engine()

        print(configuration)


if __name__ == '__main__':
    DatabaseConnector().test_methods()
