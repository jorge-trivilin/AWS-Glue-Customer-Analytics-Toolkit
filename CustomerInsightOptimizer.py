import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, when, row_number
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Spark context
sc = SparkContext.getOrCreate()

# Initialize the Glue context
glueContext = GlueContext(sc)

# Obtain the Spark session from the Glue context
spark = glueContext.spark_session

# Create and initialize a new Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data directly from the AWS Glue Catalog (previously crawled)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="crm",
    table_name="table_name_from_glue_catalog"
)

# Convert the dynamic frame to a DataFrame
df = dynamic_frame.toDF()

# Define the column data types
column_types = {
    'customer_id': StringType(),                 # 'sequencial_cliente' -> 'customer_id'
    'grouped_category': StringType(),            # 'categoria_agrupada' -> 'grouped_category'
    'category_revenue': FloatType(),             # 'receita_na_categoria' -> 'category_revenue'
    'category_purchases': FloatType()            # 'compras_na_categoria' -> 'category_purchases'
}

# Apply type conversion across the entire DataFrame
for column, dtype in column_types.items():
    df = df.withColumn(column, col(column).cast(dtype))

# Function to calculate purchase mission metrics
def calculate_metrics(df, customer_column, revenue_column, purchase_column):
    """
    Calculates metrics for each customer in each category in a DataFrame.
    """
    # Calculate the total revenue per customer
    total_customer_revenue = df.groupBy(customer_column).agg(sum(revenue_column).alias("total_revenue"))

    # Join the original DataFrame with the total revenues per customer
    df = df.join(total_customer_revenue, customer_column)

    # Calculate the percentage of revenue per category and customer
    df = df.withColumn("revenue_percentage", col(revenue_column) / col("total_revenue"))

    # Calculate the total purchases per customer
    total_customer_purchases = df.groupBy(customer_column).agg(sum(purchase_column).alias("total_purchases"))

    # Join the DataFrame with the total purchases per customer
    df = df.join(total_customer_purchases, customer_column)

    # Calculate the percentage of purchases per category and customer
    df = df.withColumn("purchase_percentage", col(purchase_column) / col("total_purchases"))

    # Calculate the final metric
    df = df.withColumn("metric", col("revenue_percentage") * col("purchase_percentage") * 100)

    return df

# Apply the calculate_metrics function to compute the metrics
df_metrics = calculate_metrics(
    df, 
    customer_column='customer_id',
    revenue_column='category_revenue',
    purchase_column='category_purchases'
)

def select_shopping_mission(df, customer_column, category_column, metric_column, revenue_column, purchase_column):
    """
    Selects the shopping mission for each customer in a DataFrame using Spark.
    """
    # Define the window specification for partitioning by customer and ordering by metric and revenue
    windowSpec = Window.partitionBy(customer_column).orderBy(
        col(metric_column).desc(),
        col(revenue_column).desc()
    )

    # Apply the row_number function to select only the first row of each group
    df = df.withColumn("row_number", row_number().over(windowSpec))

    # Filter to obtain only the first row of each group, which would be the shopping mission
    df_shopping_mission = df.filter(col("row_number") == 1).drop("row_number")

    # Rename columns for clarity
    df_shopping_mission = df_shopping_mission.withColumnRenamed(customer_column, "customer") \
                                             .withColumnRenamed(category_column, "shopping_mission") \
                                             .withColumnRenamed(revenue_column, "mission_revenue") \
                                             .withColumnRenamed(purchase_column, "mission_purchases") \
                                             .withColumnRenamed(metric_column, "mission_metric")

    return df_shopping_mission

# Apply the select_shopping_mission function to find the main shopping mission
df_mission = select_shopping_mission(
    df_metrics, 
    customer_column='customer_id', 
    category_column='grouped_category', 
    metric_column='metric', 
    revenue_column='category_revenue',
    purchase_column='category_purchases'
)

# Path in S3 where the data will be saved
path = "s3_uri_path"

# Save the DataFrame in S3 as a Parquet dataset partitioned by 'Period'
(df_mission.write
    .format("parquet")
    .option("path", path)
    .partitionBy("Period")  # Partition the data by 'Period'
    .mode("append")  # Append mode to add data without overwriting
    .option("compression", "snappy")  # Use Snappy compression for efficient storage
    .save())

# Finish the job after transformations
job.commit()
