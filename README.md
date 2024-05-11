# GlueShopperMissionMetricsOptimizer

## Descrição
O **GlueMetricsOptimizer** é um script Spark projetado para ser executado no ambiente AWS Glue. Ele otimiza e calcula métricas de compras e receitas por cliente, com base em dados históricos. Este script processa grandes volumes de dados para identificar a "missão de compra" mais relevante para cada cliente, utilizando critérios como a receita total e o número de compras em diferentes categorias.

## Recursos
- Carregamento e transformação de dados do AWS Glue Catalog.
- Cálculo de métricas personalizadas de engajamento do cliente.
- Seleção de missões de compras mais significativas para cada cliente.
- Exportação de dados processados para o Amazon S3 em formato Parquet, com compressão Snappy.

## Pré-requisitos
- AWS Account
- Configuração do AWS Glue
- Permissões de IAM apropriadas para o AWS Glue e o S3
- Bucket do S3 para armazenar os scripts e os dados de saída

## Configuração
1. **IAM Role**: Certifique-se de que a role do IAM usada pelo AWS Glue tenha permissão para acessar os recursos necessários no S3.
2. **Bucket do S3**: Crie ou especifique um bucket do S3 para armazenar o script e os dados de saída.

## Uso
### Configuração do Job no AWS Glue
1. Faça login no console da AWS e navegue até o serviço AWS Glue.
2. Crie um novo job e selecione um `IAM role` que tenha as permissões necessárias.
3. Carregue o script `GlueMetricsOptimizer.py` no editor de scripts do job ou especifique o caminho do S3 onde o script está armazenado.
4. Configure os parâmetros necessários, como o nome do job e quaisquer argumentos de script específicos.
5. Salve e execute o job para processar seus dados.

### Executando o Job
- O job pode ser executado diretamente através do console do AWS Glue ou programado para execução com base em gatilhos definidos por cron ou eventos do S3.

## Manutenção
- **Atualizações do Script**: Atualize o script no repositório e ajuste os parâmetros no job do Glue conforme necessário.
- **Monitoramento**: Monitore a execução do job através dos logs do CloudWatch para garantir que está executando como esperado.

## Contato
- **Autor**: Jorge Trivilin
- **Email**: jorge.trivilin@gmail.com
