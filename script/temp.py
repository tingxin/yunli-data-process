import boto3

# 创建EMR客户端
emr_client = boto3.client('emr', region_name='cn-northwest-1')
#spark 生成 iceberg 表，必须有个warehouse path, 一般是所有iceberg 表的前缀
warehouse = 's3://tx-emr/icebergdemo'
default_catalog = 'AwsDataCatalog'
sql = """
CREATE TABLE demo2.quick2 (
  col_timestamp timestamp COMMENT '',
  col_decimal decimal(18, 2) COMMENT '',
  col_int int COMMENT '',
  col_str string COMMENT '',
  col_month string COMMENT '',
  col_dt string COMMENT '')
USING iceberg
PARTITIONED BY (col_month, col_dt)
LOCATION 's3://tx-emr/icebergdemo/demo2/quick2'
TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='3600000',
  'parquet.charset'='UTF-8',
  'parquet.enable.dictionary'='false'
)
"""
sql2 = """
insert into demo2.quick2 values (current_timestamp(), 12.12, 9, '测试中文999'.encode(ut), '2023-01','2023-01-01')
""".encode('utf-8').decode('utf-8')

response = emr_client.add_job_flow_steps(
    JobFlowId='j-2J6RFU5NCD80K',
    Steps=[
        {
            'Name': 'Spark SQL Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--master', 'yarn',
                    '--deploy-mode', 'cluster',
                    '--conf',f'spark.sql.defaultCatalog={default_catalog}',
                    '--conf',f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                    '--conf',f'spark.sql.catalog.{default_catalog}=org.apache.iceberg.spark.SparkCatalog',
                    '--conf',f'spark.sql.catalog.{default_catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                    '--conf',f'spark.sql.catalog.{default_catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                    '--conf',f'spark.hadoop.hive.{default_catalog}.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                    '--conf',f'spark.sql.catalog.{default_catalog}.warehouse={warehouse}',
                    '--conf','spark.sql.iceberg.handle-timestamp-without-timezone=true',
                    '--conf','spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8', \
                    '--conf','spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8', \
                    '--conf','spark.sql.session.charset=UTF-8', \
                    '--conf','spark.executorEnv.LANG=en_US.UTF-8', \
                    '--conf','spark.executorEnv.LC_ALL=en_US.UTF-8', \
                    's3://tx-emr/yunlisql/sparksql.py',
                    "--name", "demo112",
                    "--sql", sql2
                ]
            }
        }
    ]
)

# 打印提交任务的响应
print(response)

# 检查响应状态码
if response['ResponseMetadata']['HTTPStatusCode'] == 200:
    print('Spark SQL任务已成功提交')
else:
    print('无法提交Spark SQL任务')


# response = emr_client.describe_step(ClusterId='your_cluster_id', StepId='your_step_id')
# state = response['Step']['Status']['State']
