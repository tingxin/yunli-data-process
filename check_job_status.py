import boto3
import time

emr_client = boto3.client('emr', region_name='cn-northwest-1')

while True:
    response = emr_client.describe_step(ClusterId='j-3E9NR8YACP6YB', StepId='s-3ETTJVSSQ61G7')
    state = response['Step']['Status']['State']
    print(state)
    if state == 'PENDING':
        print('步骤正在等待执行...')
    elif state == 'RUNNING':
        print('步骤正在执行中...')
    elif state == 'COMPLETED':
        print('步骤执行完成。')
    elif state == 'CANCELLED':
        print('步骤已取消。')
    elif state == 'FAILED':
        print('步骤执行失败。')
    elif state == 'INTERRUPTED':
        print('步骤被中断。')
    time.sleep(1)

