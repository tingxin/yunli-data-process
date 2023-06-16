import boto3

# 创建EMR客户端
emr_client = boto3.client('emr', region_name='cn-northwest-1')
#spark 生成 iceberg 表，必须有个warehouse path, 一般是所有iceberg 表的前缀
warehouse = 's3://tx-emr/icebergdemo'

response = emr_client.add_job_flow_steps(
    JobFlowId='j-2OI8QAWE9OX3H',
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
                    '--conf','spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                    '--conf','spark.sql.catalog.AwsDataCatalog=org.apache.iceberg.spark.SparkCatalog',
                    '--conf','spark.sql.catalog.AwsDataCatalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                    '--conf','spark.sql.catalog.AwsDataCatalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                    '--conf','spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                    '--conf',f'spark.sql.catalog.AwsDataCatalog.warehouse={warehouse}',
                    's3://tx-emr/yunlisql/sparksql.py',
                    "--name", "demo112",
                    "--sql", "insert into AwsDataCatalog.study.test0 select * from AwsDataCatalog.study.test5 limit 10"
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
