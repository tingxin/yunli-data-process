#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ast import main
import boto3
from datetime import date


### 这里修改你提交的SQL以及给这个SQL 任务起一个可识别的名字
TASK_NAME = 'fast1'
### 这里是SQL的列表
SQL = ["""
CREATE TABLE IF NOT EXISTS demo2.fast1 (
  col_timestamp timestamp COMMENT '',
  col_decimal decimal(18, 2) COMMENT '',
  col_int int COMMENT '',
  col_str string COMMENT '',
  col_month string COMMENT '',
  col_dt string COMMENT '')
USING iceberg
PARTITIONED BY (col_month, col_dt)
LOCATION 's3://tx-emr/icebergdemo/demo2/fast1'
TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='3600000',
  'parquet.charset'='UTF-8',
  'parquet.enable.dictionary'='false'
)
""",
"""
insert into demo2.fast1 values (current_timestamp(), 12.12, 9, '测试中文999', '2023-01','2023-01-01')
"""
]

### 这里需要修改
#spark 生成 iceberg 表，必须有个warehouse path, 一般是所有iceberg 表的前缀
WAREHOSE = 's3://tx-emr/icebergdemo'
# 存放你提交的SQL任务的S3目录
CODE_REPO = 's3://tx-emr/code'
# 目前的默认的数据目录
DEFAULT_CATALOG = 'AwsDataCatalog'
####

### 后面的代码请不要修改
def template(name:str, sql_list:list)->str:
    temp = f"""
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession

def run_sql(name, sql_list):
    with SparkSession.builder.appName(name)\
    .config('spark.jars', '/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar')\
    .enableHiveSupport().getOrCreate() as spark:
                for sql in sql_list:
                    spark.sql(sql)

if __name__ == "__main__":
    lst = {sql_list}
    run_sql(\"{name}\", lst)	
    """
    return temp


def run(name:str, sql:str):
    # 创建EMR客户端
    s3_client = boto3.client('s3', region_name='cn-northwest-1')

    text_content = template(name,sql)
    parts = CODE_REPO.split('/')
    bucket =parts[2]
    prefix = '/'.join(parts[3:])

    object_key =f"{prefix}/{name}-{date.today()}.py"
    file_path = f"s3://{bucket}/{object_key}"
    s3_client.put_object(Body=text_content, Bucket=bucket, Key=object_key)

    emr_client = boto3.client('emr', region_name='cn-northwest-1')

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
                        '--conf',f'spark.sql.defaultCatalog={DEFAULT_CATALOG}',
                        '--conf',f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                        '--conf',f'spark.sql.catalog.{DEFAULT_CATALOG}=org.apache.iceberg.spark.SparkCatalog',
                        '--conf',f'spark.sql.catalog.{DEFAULT_CATALOG}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                        '--conf',f'spark.sql.catalog.{DEFAULT_CATALOG}.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                        '--conf',f'spark.hadoop.hive.{DEFAULT_CATALOG}.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                        '--conf',f'spark.sql.catalog.{DEFAULT_CATALOG}.warehouse={WAREHOSE}',
                        '--conf','spark.sql.iceberg.handle-timestamp-without-timezone=true',
                        f'{file_path}'
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

if __name__ == "__main__":
    run(TASK_NAME, SQL)