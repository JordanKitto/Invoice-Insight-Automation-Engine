# Python Invoice Optimisation Report - Oracle Data Export Tool

## Project Purpose

This project is designed to automate the generation of the **Invoice Optimisation Report** used by the Queensland Health Statewide Accounts Payable team. It will run **nine specific Oracle SQL scripts in sequence**, each exporting key datasets used to identify issues with the OCR (Optical Character Recognition) engine that processes over **1.8 million invoices per year**.

The ultimate goal is to **improve zero-touch invoice processing rates** by providing timely insights and highlighting common exceptions and errors in vendor setup, payment terms, and invoice formatting. This tool replaces manual Excel-based workflows, significantly reducing overhead and increasing processing efficiency.


<img width="389" height="211" alt="image" src="https://github.com/user-attachments/assets/d65f84e5-5dbf-44a7-a750-09130c040cb1" />

---
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
    
Future Enhancements

    Add CLI arguments to run selected SQL scripts

    Combine all exports into a zipped archive

    Integrate post-processing/analysis phase for exception tracking

    Schedule via Windows Task Scheduler or cron for automation
