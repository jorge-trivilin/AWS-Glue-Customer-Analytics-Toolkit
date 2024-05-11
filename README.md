# Glue Shopper Mission Metrics Optimizer

A Spark script designed to be executed in the AWS Glue environment. It optimizes and calculates purchase and revenue metrics per customer, based on historical data. This script processes large volumes of data to identify the most relevant "shopping mission" for each customer, using criteria such as total revenue and the number of purchases across different categories.

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

## Contact
- **Author**: Jorge Trivilin
- **Email**: [jorge.trivilin@gmail.com](mailto:jorge.trivilin@gmail.com)
