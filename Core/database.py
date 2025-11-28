import oracledb

class OracleConnection:
    def __init__(self, config: dict):
        # Define the connection authentication details from credential dictionary
        self.dsn = oracledb.makedsn(
            config["hostname"],
            config["port"],
            service_name=config["service_name"]
        )
        self.user = config["user"]
        self.password = config["password"]
        self.conn = None

     # Connect to the database and return connection for reuseability
    def connect(self):
        self.conn = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=self.dsn
         )
        return self.conn
        
    # Run query and fetch all rows and headers
    def run_query(self, query: str):
        # set cursor connection
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            # List of column names
            columns = [col[0] for col in cursor.description] 
            # List of tuples
            rows = cursor.fetchall()
            return columns, rows
        finally:
            # Close the connection regardless of errors
            cursor.close()

    # Run query and fetch in batches for improve performance
    def run_in_batches(self, query: str, batch_size: int = 10000):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            # Use a generator to run data one batch at a time
            yield columns, rows


    
    def get_row_count(self, query: str) -> int:
        """
        Returns the total number of rows the original query would return.
        """
        count_query = self.build_count_query(query)
        cursor = self.conn.cursor()
        try:
            cursor.execute(count_query)
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()

    @staticmethod
    def build_count_query(query: str) -> str:
        """
        Converts the provided SQL query into a COUNT(*) version by:
        - Finding the FROM clause
        - Wrapping the original query as a subquery
        """
        return f"SELECT COUNT(*) FROM ({query}) subquery"

    # Close connection efficiently
    def close(self):
        if self.conn:
            self.conn.close()


        