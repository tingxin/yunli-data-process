aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole 
--applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Trino 
--bootstrap-actions '[{"Path":"s3://ziweiningxia/BA/trino/ba_trino.sh","Name":"trinoba"}]' 
--ebs-root-volume-size 50 
--ec2-attributes '{"KeyName":"RSA for AmazonLiunx2","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-07125125fd3788e41","EmrManagedSlaveSecurityGroup":"sg-0a7fcb26389dc46ef","EmrManagedMasterSecurityGroup":"sg-0ec3c16c551611dcf"}' 
--service-role EMR_DefaultRole 
--release-label emr-6.10.0 
--log-uri 's3n://aws-logs-777665963006-cn-northwest-1/elasticmapreduce/' 
--name 'trino' 
--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge",
"Configurations":[
    {"Classification":"core-site","Properties":{"s3a.endpoint":"s3.cn-northwest-1.amazonaws.com.cn"}}],"Name":"主实例组 - 1"},
{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Configurations":[{"Classification":"core-site","Properties":{"s3a.endpoint":"s3.cn-northwest-1.amazonaws.com.cn"}}],"Name":"核心实例组 - 2"}]' 
--configurations '[{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' 
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION 
--region cn-northwest-1