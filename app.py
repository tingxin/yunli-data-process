import boto3

# 创建EMR客户端
emr_client = boto3.client('emr', region_name='cn-northwest-1')

response = emr_client.add_job_flow_steps(
    JobFlowId='j-3E9NR8YACP6YB',
    Steps=[
        {
            'Name': 'Spark SQL Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--class', 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2',
                    '--master', 'yarn',
                    '--deploy-mode', 'client',
                    '--conf', 'spark.sql.hive.thriftServer.singleSession=true',
                    's3://tx-emr/yunlisql/demo.sql'
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
