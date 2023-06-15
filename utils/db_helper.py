import boto3

from utils.global_variables import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Connect function for DynamoDB
def connect_dynamodb():
    dynamodb = boto3.resource('dynamodb',
        aws_access_key_id = AWS_ACCESS_KEY_ID, 
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY, 
        region_name = 'us-east-1'
    )
    return dynamodb