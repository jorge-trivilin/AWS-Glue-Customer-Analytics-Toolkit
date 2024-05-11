# Glue Shopper Mission Metrics Optimizer

*The Glue Shopper Mission Metrics Optimizer is a sophisticated AWS Glue Spark script engineered for data analysts and data scientists working with crm. This script leverages the power of big data to optimize and calculate key customer metrics, such as purchase frequency and revenue generation, across various product categories. This script processes large volumes of data to identify the most relevant "shopping mission" for each customer, using criteria such as total revenue and the number of purchases across different categories.*

All scripts in this repository have been anonymized for security and privacy. This includes category names, data paths, and any other specific identifiers, which have been replaced or generalized. Please adapt the column names and data formats to fit your specific dataset.

![Version](https://img.shields.io/badge/version-1.0.0-blue)

### About The Creator

*This project was created by Jorge Trivilin.* 

*Connect with Jorge here:*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Jorge_Trivilin-blue?style=flat&logo=linkedin)](https://www.linkedin.com/in/jorgetrivilin/)
[![GitHub](https://img.shields.io/badge/GitHub-jorgetrivilin-lightgrey?style=flat&logo=github)](https://github.com/jorge-trivilin/)

## Features
- **Data Loading and Transformation**: Loads and transforms data from the AWS Glue Catalog to prepare it for analysis.
- **Metric Calculation**: Calculates customized customer engagement metrics to assess the effectiveness of marketing and sales strategies.
- **Mission Selection**: Identifies and selects the most significant shopping missions for each customer, helping businesses tailor their marketing efforts more effectively.
- **Data Export**: Exports processed data to Amazon S3 in Parquet format, using Snappy compression for efficient storage and retrieval.

## Prerequisites
- **AWS Account**: Required for access to AWS services.
- **AWS Glue Setup**: Must be configured to run Spark scripts.
- **IAM Permissions**: Adequate permissions for AWS Glue and Amazon S3 are necessary to execute the script and store outputs.
- **S3 Bucket**: Needed to store the scripts and the data outputs.

## Setup
- **IAM Role**: Ensure that the IAM role used by AWS Glue has permissions to access the necessary resources on S3.
- **S3 Bucket**: Create or specify an S3 bucket to store the script and output data.

## Usage
### Setting up the Job in AWS Glue
1. Log in to the AWS console and navigate to the AWS Glue service.
2. Create a new job and select an IAM role that has the necessary permissions.
3. Upload the `GlueMetricsOptimizer.py` script to the job's script editor or specify the S3 path where the script is stored.
4. Configure the necessary parameters, such as the job name and any specific script arguments.
5. Save and execute the job to process your data.

### Running the Job
- The job can be executed directly through the AWS Glue console or scheduled to run based on triggers set by cron or S3 events.
