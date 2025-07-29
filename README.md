Python Invoice Optimisation Report - Oracle Data Export Tool

This project is a modular Python tool that connects to an Oracle database, runs a SQL query from a file, and exports the results to a CSV. It handles large datasets in batches and includes a timer and estimated time remaining.
Features

    Connects to Oracle using oracledb

    Executes SQL query from .sql file

    Exports results to timestamped CSV

    Streams data in batches to reduce memory usage

    Displays elapsed time while running

    Estimates time remaining using total row count

    Uses object-oriented design with clear file separation


    Install dependencies:
    pip install oracledb pandas

    Configure your database settings in Config/db_config.py:
    DB_CONFIG = {
    "hostname": "your_host",
    "port": 1521,
    "service_name": "your_service",
    "user": "your_username",
    "password": "your_password"
    }

    Add your SQL query to SQL/vendor_master.sql.

Usage

Run the script:
python main.py

The script will:

    Connect to Oracle

    Run a COUNT(*) version of the query to get total row estimate

    Stream results in batches (default 10,000)

    Display elapsed time and estimated time remaining

    Save results to Output_Files/vendor_master_<timestamp>.csv

Customization

    To change batch size, update batch_size in main.py

    To use a different query, change SQL file in main.py

    To change file name format, adjust output_file naming in main.py

Notes

    Efficient memory usage with fetch generator

    Threaded live timer

    ETA calculated from COUNT(*) before batch fetch

    Clean error handling and graceful shutdown
