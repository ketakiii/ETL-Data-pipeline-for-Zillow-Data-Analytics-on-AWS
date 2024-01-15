# import libraries
import boto3
import json

# representation of the s3
s3_client = boto3.client('s3')

# lambda function with the event 
def lambda_handler(event, context):
    # the source bucket 
    source_bucket = event['Records'][0]['s3']['bucket']['name']     
    # the file name that enters the s3 bucket 
    object_key = event['Records'][0]['s3']['object']['key']
    
    # target bucket 
    target_bucket = 'copy-of-raw-json-buckett3'
    # copy source
    copy_source = {'Bucket':source_bucket, 'Key':object_key}
    
    # waiter 
    waiter = s3_client.get_waiter('object_exists')
    # wait until the file exists in the bucket
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    # copy the object in the target bucket with the same name 
    s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)
    
    # return if successully copied to the target bucket 
    return {
        'statusCode': 200,
        'body': json.dumps('Copy completed successfully')
    }
