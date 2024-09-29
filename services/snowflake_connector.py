import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError

class SnowflakeConnector:
    def __init__(self, config):
        self.config = config

    def execute_query(self, query):
        try:
            conn = snowflake.connector.connect(
                user=self.config['SNOWFLAKE_USER'],
                password=self.config['SNOWFLAKE_PASSWORD'],
                account=self.config['SNOWFLAKE_ACCOUNT'],
                warehouse=self.config['SNOWFLAKE_WAREHOUSE'],
                database=self.config['SNOWFLAKE_DATABASE'],
                schema=self.config['SNOWFLAKE_SCHEMA']
            )
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return results, columns

        except ProgrammingError as pe:
            print(f"Programming error occurred: {pe}")
            # Log and handle the error
        except DatabaseError as de:
            print(f"Database error occurred: {de}")
            # Log and handle the error
        except Exception as e:
            print(f"An error occurred: {e}")
            # Log and handle the error
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
