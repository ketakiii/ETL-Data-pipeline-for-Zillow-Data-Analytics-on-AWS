# import libraries
import boto3
import json
import pandas as pd 

# representation of the s3
s3_client = boto3.client('s3')

# lambda function
def lambda_handler(event, context):
     # the source bucket 
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    # the file name that enters the s3 bucket
    object_key = event['Records'][0]['s3']['object']['key']
    
    # target bucket 
    target_bucket = 'cleaned-data-csv-bucket3'
    # file name without json string
    target_file_name = object_key[:-5]
    print(target_file_name)
    
    # waiter 
    waiter = s3_client.get_waiter('object_exists')
    # wait until the file exists in the bucket
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    # gets the object from the s3 bucket
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response)
    # data in the response body
    data = response['Body']
    print(data)
    data = response['Body'].read().decode('utf-8')
    print(data)
    # have the data in a json format 
    data = json.loads(data)
    print(data)
    
    # create a list from looping over the dictionaries in the json
    f = []
    for i in data['results']:
        f.append(i)
    
    # creating a dataframe 
    df = pd.DataFrame(f)
    
    # select specified columns 
    selected_cols = ['bathrooms', 'bedrooms', 'city', 'country', 'homeType', 
    'homeStatus', 'livingArea', 'price', 'rentZestimate', 'zipcode', 
    'latitude', 'longitude', 'taxAssessedValue', 'isPremierBuilder']
    df = df[selected_cols]
    
    # convert to csv
    csv_data = df.to_csv(index=False)
    
    # upload the csv to s3
    # the target bucket 
    bucket_name = target_bucket
    # the file name 
    object_key = f"{target_file_name}.csv"
    # adds the object to the s3
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)
    
    # return message on success
    return {
        'statusCode': 200,
        'body': json.dumps('csv conversion and upload to the s3 complete!')
    }
