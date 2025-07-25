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

    # Close connection efficiently
    def close(self):
        if self.conn:
            self.conn.close()


        