**Glue Shopper Mission Metrics Optimizer**

The GlueMetricsOptimizer is a Spark script designed to be executed in the AWS Glue environment. It optimizes and calculates purchase and revenue metrics per customer, based on historical data. This script processes large volumes of data to identify the most relevant "shopping mission" for each customer, using criteria such as total revenue and the number of purchases across different categories.

**Features**
- Loading and transforming data from the AWS Glue Catalog.
- Calculation of customized customer engagement metrics.
- Selection of the most significant shopping missions for each customer.
- Export of processed data to Amazon S3 in Parquet format, using Snappy compression.

**Prerequisites**
- AWS Account
- AWS Glue setup
- Appropriate IAM permissions for AWS Glue and S3
- S3 bucket to store the scripts and output data

**Setup**
- **IAM Role**: Ensure that the IAM role used by AWS Glue has permissions to access the necessary resources on S3.
- **S3 Bucket**: Create or specify an S3 bucket to store the script and output data.

**Usage**
- **Setting up the Job in AWS Glue**
  - Log in to the AWS console and navigate to the AWS Glue service.
  - Create a new job and select an IAM role that has the necessary permissions.
  - Upload the GlueMetricsOptimizer.py script to the job's script editor or specify the S3 path where the script is stored.
  - Configure the necessary parameters, such as the job name and any specific script arguments.
  - Save and execute the job to process your data.

- **Running the Job**
  - The job can be executed directly through the AWS Glue console or scheduled to run based on triggers set by cron or S3 events.

**Maintenance**
- **Script Updates**: Update the script in the repository and adjust the parameters in the Glue job as needed.
- **Monitoring**: Monitor the job's execution through CloudWatch logs to ensure it is running as expected.

**Contact**
- **Author**: Jorge Trivilin
- **Email**: jorge.trivilin@gmail.com

---
