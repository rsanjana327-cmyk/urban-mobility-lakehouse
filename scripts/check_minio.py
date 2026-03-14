import boto3
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

print('=' * 50)
print('ALL FILES ACROSS ALL BUCKETS:')
print('=' * 50)

buckets = ['bronze', 'gold', 'lakehouse', 'raw', 'silver']

for bucket in buckets:
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get('Contents', [])
        if objects:
            print(f'\nBucket: {bucket}')
            for obj in objects:
                print(f'  {obj["Key"]} ({obj["Size"]/1024/1024:.1f} MB)')
        else:
            print(f'\nBucket: {bucket} — EMPTY')
    except Exception as e:
        print(f'\nBucket: {bucket} — ERROR: {e}')