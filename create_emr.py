import boto3

emr_client = boto3.client('emr', region_name='cn-northwest-1')

# 配置集群参数
cluster_params = {
    'Name': 'script_emr_pro3',
    'ReleaseLabel': 'emr-6.5.0',  # EMR 版本号
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],  # 需要的应用程序
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',  # Master 实例类型
                'InstanceCount': 1
            },
            {
                'Name': 'Core',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',  # Core 实例类型
                'InstanceCount': 2
            },
             {
                'Name': 'Task',
                'Market': 'SPOT',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',  # Spot 实例类型
                'InstanceCount': 2
            }
        ],
        'Ec2KeyName': 'tingxin_dev', # Spot 实例类型
        'KeepJobFlowAliveWhenNoSteps': True,  # 当没有步骤时保持集群活动
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-32aa255b',  # 子网 ID
        'EmrManagedMasterSecurityGroup': 'sg-0d1fe3d3562546cf0',
        'EmrManagedSlaveSecurityGroup': 'sg-0bf2eae38501b4fac'
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'VisibleToAllUsers': True,
    'Steps': [],
    'BootstrapActions': [],
    'Configurations': [],
    'EbsRootVolumeSize': 30
}

# 创建集群
response = emr_client.run_job_flow(**cluster_params)

# 获取集群 ID
cluster_id = response['JobFlowId']

print('集群创建成功，集群 ID:', cluster_id)
