from ast import main
import boto3
import argparse

def get_ip_info(cluster_id:str):
    # 创建EMR客户端
    emr_client = boto3.client('emr', region_name='cn-northwest-1')

    # 获取集群信息
    response = emr_client.describe_cluster(ClusterId=cluster_id)

    print(response['Cluster'])
    # 获取Master节点信息
    master_public_dns = response['Cluster']['MasterPublicDnsName']


    # 获取Master节点的公有IP和私有IP
    ec2_client = boto3.client('ec2', region_name='cn-northwest-1')
    response = ec2_client.describe_instances(
        Filters=[
            {
                'Name': 'dns-name',
                'Values': [master_public_dns]
            }
        ]
    )

    master_private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']


    print('Master节点的公有IP:', master_public_dns)
    print('Master节点的私有IP:', master_private_ip)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cluster_id', help="cluster_id")
    args = parser.parse_args()
    get_ip_info(args.cluster_id)