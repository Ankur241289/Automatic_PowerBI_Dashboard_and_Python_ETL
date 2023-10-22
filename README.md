# Automatic_Python_ETL
This repository contains files and code snippet for Automatic Python ETL for importing data from various sources, transformating and storage

# ETL (Extract, Transform, Load) Pipeline for Financial Data

This Python ETL pipeline extracts financial data from a source S3 bucket, performs data transformations, and loads the processed data into a target S3 bucket in the form of Parquet files. The pipeline is designed to handle historical financial data and perform key data processing steps.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Overview](#pipeline-overview)
- [Contributing](#contributing)

## Features

- Extract financial data from an S3 source bucket.
- Merge and process data from multiple CSV files.
- Calculate daily opening and closing prices.
- Calculate percentage change in closing prices.
- Aggregate the data from "hourly" to "daily"
- Store processed data in an S3 target bucket in Parquet format.

## Prerequisites

- Python 3.9.5
- AWS account and IAM credentials
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Pandas](https://pandas.pydata.org/)
- [NumPy](https://numpy.org/)
- pyarrow

## Installation

1. Clone the repository to your local machine:
   ```sh
   git clone https://github.com/Ankur241289/Automatic_Python_ETL.git

3. Install the required Python packages using pip:
   ```sh
   pip install -r requirements.txt

5. Configure your AWS credentials by setting environment variables or using `aws configure`.

## Usage

1. Edit the script to configure the source and target S3 buckets and date range for data extraction.

2. Run the ETL script:
   ```sh
   Python ETL Connecting to Multiple files from S3 bucket Quick and Dirty.py
   
4. Processed data will be stored in the target S3 bucket in Parquet format.

## Pipeline Overview
1. Connect to source and target S3 buckets using AWS credentials.

2. Extract financial data from the source S3 bucket based on a specified date range.

3. Merge and process data to calculate daily opening and closing prices.

4. Calculate the percentage change in closing prices from the previous day.

5. Aggregate the data from "hourly" to "daily"

6. Store the processed data in the target S3 bucket in Parquet format.
   
## Contributing
Contributions are welcome! Feel free to open issues or pull requests to improve this ETL pipeline.
