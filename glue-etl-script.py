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

# Load data directly from the AWS Glue Catalog (previously crawled from another SQL transformation)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="crm",
    table_name="table_name_from_glue_catalog"
)

# Convert the dynamic frame to a DataFrame
df = dynamic_frame.toDF()

# Define the column data types
column_types = {
    'sequencial_cliente': StringType(),
    'categoria_agrupada': StringType(),
    'receita_na_categoria': FloatType(),
    'compras_na_categoria': FloatType()
}

# Apply type conversion across the entire DataFrame
for column, dtype in column_types.items():
    df = df.withColumn(column, col(column).cast(dtype))

# Function to calculate purchase mission metrics
def calcular_metrica(df, coluna_cliente, coluna_receita, coluna_compras):
    """
    Calculates the metrics percentual_receita, percentual_compras, and metrica
    for each customer in each category in a DataFrame, using generic column names.
    """
    # Calculate the total revenue per customer
    total_receita_cliente = df.groupBy(coluna_cliente).agg(sum(coluna_receita).alias("total_receita"))

    # Join the original DataFrame with the total revenues per customer
    df = df.join(total_receita_cliente, coluna_cliente)

    # Calculate the percentage of revenue per category and customer
    df = df.withColumn("percentual_receita", col(coluna_receita) / col("total_receita"))

    # Calculate the total purchases per customer
    total_compras_cliente = df.groupBy(coluna_cliente).agg(sum(coluna_compras).alias("total_compras"))

    # Join the DataFrame with the total purchases per customer
    df = df.join(total_compras_cliente, coluna_cliente)

    # Calculate the percentage of purchases per category and customer
    df = df.withColumn("percentual_compras", col(coluna_compras) / col("total_compras"))

    # Calculate the final metric
    df = df.withColumn("metrica", col("percentual_receita") * col("percentual_compras") * 100)

    return df

# Apply the calcular_metrica function to compute the metrics
df_metricas = calcular_metrica(
    df, 
    coluna_cliente='sequencial_cliente',
    coluna_receita='receita_na_categoria',
    coluna_compras='compras_na_categoria'
)

def selecionar_missao_compra(df, coluna_cliente, coluna_categoria, coluna_metrica, coluna_receita, coluna_compras):
    """
    Selects the shopping mission for each customer in a DataFrame using Spark.
    """
    # Define the window specification for partitioning by customer and ordering by metric and revenue
    windowSpec = Window.partitionBy(coluna_cliente).orderBy(
        col(coluna_metrica).desc(),
        col(coluna_receita).desc()
    )

    # Apply the row_number function to select only the first row of each group
    df = df.withColumn("row_number", row_number().over(windowSpec))

    # Filter to obtain only the first row of each group, which would be the shopping mission
    df_missao_compra = df.filter(col("row_number") == 1).drop("row_number")

    # Rename columns as necessary for clarity
    df_missao_compra = df_missao_compra.withColumnRenamed(coluna_cliente, "cliente") \
                                       .withColumnRenamed(coluna_categoria, "missão_compra") \
                                       .withColumnRenamed(coluna_receita, "receita_missão") \
                                       .withColumnRenamed(coluna_compras, "compras_missão") \
                                       .withColumnRenamed(coluna_metrica, "metrica_missão")

    return df_missao_compra

# Apply the selecionar_missao_compra function to find the main shopping mission
df_missao = selecionar_missao_compra(
    df_metricas, 
    coluna_cliente='sequencial_cliente', 
    coluna_categoria='categoria_agrupada', 
    coluna_metrica='metrica', 
    coluna_receita='receita_na_categoria',
    coluna_compras='compras_na_categoria'
)

# Path in S3 where the data will be saved
path = "s3_uri_path"

# Save the DataFrame in S3 as a Parquet dataset partitioned by 'safra'
(df_missao.write
    .format("parquet")
    .option("path", path)
    .partitionBy("safra")  # Partition the data by 'safra'
    .mode("append")  # Append mode to add data without overwriting
    .option("compression", "snappy")  # Use Snappy compression for efficient storage
    .save())

# Finish the job after transformations
job.commit()
