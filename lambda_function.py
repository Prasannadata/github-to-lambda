import json
import boto3
import pandas as pd

def lambda_handler(event, context):
    # Extract bucket and key from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Retrieve the JSON file from S3
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data)
    
    # Filter delivery records based on status
    filtered_df = df[df['status'].isin(['delivered', 'cancelled'])]
    
    # Convert filtered data back to JSON
    filtered_data = filtered_df.to_dict(orient='records')
    
    # Save filtered data to another S3 bucket
    target_bucket = 'doordash-target-zn'
    target_key = key.split('/')[-1]  # Keep the same filename
    s3.put_object(Bucket=target_bucket, Key=target_key, Body=json.dumps(filtered_data))
    
    # Publish a notification via SNS
    #sns = boto3.client('sns')
    #sns.publish(
     #   TopicArn='YOUR_SNS_TOPIC_ARN',
      #  Subject='Delivery Data Processing Outcome',
       # Message='Filtered delivery data has been processed and saved successfully.'
    #)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }
