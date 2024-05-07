from psycopg2 import pool
import pandas as pd
import config

class DatabasePool:
    def __init__(self):
        self.connection_pool = pool.SimpleConnectionPool(
            1, 10, 
            user=config.postgres['user'],
            password=config.postgres['passwd'],
            host=config.postgres['host'],
            port=config.postgres['port'],
            database=config.postgres['dbname'],
            sslmode='verify-full'
        )

    def execute_query(self, query):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def query_to_dataframe(self, query):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def close_all_connections(self):
        self.connection_pool.closeall()