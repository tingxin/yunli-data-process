import boto3

emr_client = boto3.client('emr', region_name='cn-northwest-1')
# 停止 EMR 集群
response = emr_client.terminate_job_flows(
    JobFlowIds=['j-3E9NR8YACP6YB']
)

# 打印停止操作的响应
print(response)
