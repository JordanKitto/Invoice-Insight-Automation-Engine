# Python Invoice Optimisation Report - Oracle Data Export Tool

## Project Purpose

## Overview

This project powers the Invoice Optimisation Report for the Queensland Health Statewide Accounts Payable team, automating the daily extraction and analysis of critical invoice exception data from SAP, DSS, and other upstream sources.

Each year, Queensland Health processes over 1.8 million invoices totaling more than $10.56 billion AUD. Despite adopting OCR (Optical Character Recognition) technology with an expected 80% zero-touch success rate, performance remains stagnant at ~30% even after 5 years.

Through this project, we’ve:

    Identified 9 systemic OCR failure points, including:

    Missing or incorrectly read Company Code, ABN, Invoice Number, Amount, Vendor Name, and Vendor Number.

    Replaced manual Excel- and KNIME-based workflows with a fully automated Python + SQL pipeline.

    Reduced processing time from 2.5 hours daily to under 30 minutes, freeing up staff and computing resources.

    Scheduled automation to run before business hours, ensuring timely delivery of insights to SAP developers and stakeholders.

The end-to-end system executes nine custom Oracle SQL scripts, processes and exports datasets in batches, and tracks key performance indicators on OCR failures to guide SAP enhancement work.

## Why This Matters

This tool directly supports multi-million dollar efficiency initiatives within Queensland Health by:

    Highlighting OCR blind spots preventing automatic invoice posting

    Delivering actionable analytics to the SAP development team

    Providing visibility into statewide vendor compliance and data hygien

---
## Image of automated script running in terminal
<img width="529" height="529" alt="image" src="https://github.com/user-attachments/assets/8aabd909-ba28-482e-9a3f-d2859d048bd5" />

---

## Features

- **Modular OOP Design**  
  Each job (e.g., `vendor_master`, `transaction_master`) is isolated in its own runner class using single-responsibility principles. Utilities like timers, progress tracking, flag files, and exports are abstracted into reusable modules.

- **Oracle Integration**  
  Uses `oracledb` to connect to an Oracle database with secure credentials stored in a centralized `Config/db_config.py`.

- **Batch Query Execution**  
  Streams large datasets in **batches of 10,000 rows** to prevent memory overflows and improve performance.

- **Live Progress Tracking**  
  Displays both real-time progress (rows fetched) and estimated time remaining using a `COUNT(*)` pre-query.
  
- **Export to CSV**  
  Automatically saves results into cleanly named CSV files under `Output_Files/`, with optional timestamping.

- **Flag-Based Orchestration**  
  Each job writes a `.txt` flag file (e.g., `done.txt`) to coordinate the sequence of multi-job pipelines.

- **Shared DB Connection**  
  Optimized to connect to the database **once per run**, reducing latency and preventing reconnection issues.

- **Clear Error Handling**  
  Logs descriptive errors and ensures graceful shutdown of open resources.

---

## Installation

- pip install oracledb pandas.
---

## Configuration

- Configure your database settings in Config/db_config.py:
```
DB_CONFIG = {
    "hostname": "your_host",
    "port": 1521,
    "service_name": "your_service",
    "user": "your_username",
    "password": "your_password"
}


Add your SQL query to:
SQL/vendor_master.sql
```
---

## Folder Structure

```.
├── Config/
│   └── db_config.py
├── Core/
│   └── database.py
├── Job_Runner/
│   ├── vendor_master_runner.py
│   ├── transaction_master_runner.py
│   └── orchestration_runner.py
├── Utils/
│   ├── export.py
│   ├── flag_file.py
│   ├── progress.py
│   └── timer.py
├── SQL/
│   └── vendor_master.sql
├── Output_Files/
├── main.py
```
---
## Usage

Call python main.py in the terminal

The script will:

    Connect to Oracle once

    Run each export job (e.g., Vendor Master, Transaction Master)

    Count total rows via COUNT(*)

    Stream results in batches

    Track progress and ETA

    Save results to .csv

    Create a done.txt flag when each job is complete
---

## Customization
```
Option	Location
SQL file to run	SQL/*.sql
Batch size	Core/database.py → run_in_batches(batch_size=...)
Output file format	Defined inside each runner class
Timeout handling	orchestration_runner.py → run_step() logic
```
---

## Notes

    Efficient memory use via Python generators

    Threaded timer for accurate wall-time reporting

    Modular design for future extensibility

    Explicit file/directory validation for safety
---

## Future Enhancements

    Add CLI interface to select and run specific jobs

    Export all outputs into a single ZIP archive

    Integrate exception reporting or summary analysis

    Add logging framework (e.g., loguru, logging)

    Deploy as a Windows Task or cron job for automation

    Build Power BI hooks for post-export analysis
---

## Why This Project Matters

    Build maintainable ETL-style automation

    Manage large datasets from enterprise systems like Oracle

    Design clean, modular, production-ready Python code

    Apply concepts like batch processing, ETA logic, and orchestration

    Prepare for roles in Data Engineering, Automation, or Backend Python Development
