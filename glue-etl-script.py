import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as sum, when, row_number
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar os dados diretamente do AWS Glue Catalog (trazidos pelo crawler rodado anteriormente)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database = "crm",
    table_name = "missao_safras_client_category_purchase_revenue_data"
)
df = dynamic_frame.toDF()

# definição dos tipos de colunas 
column_types = {
    'sequencial_cliente': StringType(),
    'categoria_agrupada': StringType(),
    'receita_na_categoria': FloatType(),
    'compras_na_categoria': FloatType()
}

# Aplicar a conversão de tipos em todo o DataFrame
for column, dtype in column_types.items():
    df = df.withColumn(column, col(column).cast(dtype))

# função para calcular a métrica da missão compras
def calcular_metrica(df, coluna_cliente, coluna_receita, coluna_compras):
    """
    Calcula as métricas percentual_receita, percentual_compras e metrica
    para cada cliente em cada categoria em um DataFrame, usando nomes de coluna genéricos.
    """
    # Cálculo da receita total por cliente
    total_receita_cliente = df.groupBy(coluna_cliente).agg(sum(coluna_receita).alias("total_receita"))

    # Junção do DataFrame original com a soma de receitas por cliente
    df = df.join(total_receita_cliente, coluna_cliente)

    # Cálculo do percentual de receita por categoria e cliente
    df = df.withColumn("percentual_receita", col(coluna_receita) / col("total_receita"))

    # Cálculo do total de compras por cliente
    total_compras_cliente = df.groupBy(coluna_cliente).agg(sum(coluna_compras).alias("total_compras"))

    # Junção do DataFrame com a soma de compras por cliente
    df = df.join(total_compras_cliente, coluna_cliente)

    # Cálculo do percentual de compras por categoria e cliente
    df = df.withColumn("percentual_compras", col(coluna_compras) / col("total_compras"))

    # Cálculo da métrica
    df = df.withColumn("metrica", col("percentual_receita") * col("percentual_compras") * 100)

    return df

# Aplica a função calcular_metrica para calcular as métricas
df_metricas = calcular_metrica(
    df, 
    coluna_cliente='sequencial_cliente',
    coluna_receita='receita_na_categoria',
    coluna_compras='compras_na_categoria'
)

def selecionar_missao_compra(df, coluna_cliente, coluna_categoria, coluna_metrica, coluna_receita, coluna_compras):
    """
    Seleciona a missão de compra para cada cliente em um DataFrame usando Spark.
    """
    # Definindo a janela de partição por cliente e ordenando por métrica e receita
    windowSpec = Window.partitionBy(coluna_cliente).orderBy(
        col(coluna_metrica).desc(),
        col(coluna_receita).desc()
    )

    # Aplicando a função row_number para conseguir selecionar apenas a primeira linha de cada grupo
    df = df.withColumn("row_number", row_number().over(windowSpec))

    # Filtrando para obter apenas a primeira linha de cada grupo, que seria a missão de compra
    df_missao_compra = df.filter(col("row_number") == 1).drop("row_number")

    # Renomeando colunas conforme necessário
    df_missao_compra = df_missao_compra.withColumnRenamed(coluna_cliente, "cliente") \
                                       .withColumnRenamed(coluna_categoria, "missão_compra") \
                                       .withColumnRenamed(coluna_receita, "receita_missão") \
                                       .withColumnRenamed(coluna_compras, "compras_missão") \
                                       .withColumnRenamed(coluna_metrica, "metrica_missão")

    return df_missao_compra

# Aplica a função selecionar_missao_compra para encontrar a missão de compra principal
df_missao = selecionar_missao_compra(
    df_metricas, 
    coluna_cliente='sequencial_cliente', 
    coluna_categoria='categoria_agrupada', 
    coluna_metrica='metrica', 
    coluna_receita='receita_na_categoria',
    coluna_compras='compras_na_categoria'
)

# Caminho no S3 onde os dados serão salvos
path = "s3://sagemaker-missao-compra/mc-metric-output-safras/safras_combinadas.parquet"

# Salvar o DataFrame no S3 como um dataset Parquet particionado por 'safra'
(df_missao.write
    .format("parquet")
    .option("path", path)
    .partitionBy("safra")  # Particiona os dados por 'safra'
    .mode("append")  # Modo append para adicionar dados sem sobrescrever
    .option("compression", "snappy")  # Usa compressão Snappy para armazenamento eficiente
    .save())


# Finaliza o job após as transformações
job.commit()
