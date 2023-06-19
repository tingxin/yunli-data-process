import boto3


client = boto3.client(
    'emr',
    region_name='cn-northwest-1',
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY'
)


emr_client = boto3.client('emr', region_name='cn-northwest-1')

# 配置集群参数
cluster_params = {
    'Name': 'emr3',
    'ReleaseLabel': 'emr-6.10.0',  # EMR 版本号
    'Applications': [
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Hive'
        },
        {
            'Name': 'Spark'
        },
        {
            'Name': 'Trino'
        }
    ],  # 需要的应用程序
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
    'BootstrapActions': [{
        'Name': 'TrinoCatalog',
        'ScriptBootstrapAction': {
            'Path': 's3://tx-emr/plugin/iceberg/boots.sh'
        }
    }],
    'Configurations': [],
    'EbsRootVolumeSize': 30, # 本地磁盘大小
    'LogUri':'s3://tx-emr/log/',
    'Configurations': [
         {
            'Classification': 'core-site',
            'Properties': {
                's3a.endpoint': 's3.cn-northwest-1.amazonaws.com.cn'
            }
        },
        {
            'Classification': 'hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        },
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        },
        {
            "Classification":"iceberg-defaults",
            "Properties":{"iceberg.enabled":"true"}
        }
    ],
    'ManagedScalingPolicy':{ # AutoSCalingPolicy
        "ComputeLimits":{
            "MaximumOnDemandCapacityUnits":2,
            "UnitType":"Instances",
            "MaximumCapacityUnits":6,
            "MinimumCapacityUnits":2,
            "MaximumCoreCapacityUnits":2
        }
    }
}

# 创建集群
response = emr_client.run_job_flow(**cluster_params)

# 获取集群 ID
cluster_id = response['JobFlowId']

print('集群创建成功，集群 ID:', cluster_id)
