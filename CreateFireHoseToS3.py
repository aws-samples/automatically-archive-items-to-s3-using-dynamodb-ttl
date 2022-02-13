import json
import logging
import time
import boto3
import sys
from botocore.exceptions import ClientError

def main():
    # arg 1: 'arn:aws:s3:::reservationfirehosedestinationbucket'
    # arg 2: 'firehose_to_s3_stream'
    # arg 3: 'firehose_to_s3' 
    
    # Assign these values before running the program
    # If the specified IAM role does not exist, it will be created
    bucket_arn = sys.argv[1]
    firehose_name = sys.argv[2]
    iam_role_name = sys.argv[3]

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # If Firehose doesn't exist, create it
    if not firehose_exists(firehose_name):
        # Create a Firehose delivery stream to S3. The Firehose will receive
        # data from direct puts.
        firehose_arn = create_firehose_to_s3(firehose_name, bucket_arn, iam_role_name)
        
        if firehose_arn is None:
            exit(1)
            
        logging.info('Created Firehose delivery stream to S3: {firehose_arn}')

        # Wait for the stream to become active
        if not wait_for_active_firehose(firehose_name):
            exit(1)
            
        logging.info('Firehose stream is active')

def firehose_exists(firehose_name):
    
   # Get the description of the Firehose
    if get_firehose_arn(firehose_name) is None:
        return False
    return True


def get_firehose_arn(firehose_name):
    
    # Get the description of the Firehose
    firehose_client = boto3.client('firehose')
    try:
        result = firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['DeliveryStreamDescription']['DeliveryStreamARN']



def wait_for_active_firehose(firehose_name):
    
    # Wait until the stream is active
    firehose_client = boto3.client('firehose')
    while True:
        try:
            # Get the stream's current status
            result = firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_name)
        except ClientError as e:
            logging.error(e)
            return False
        status = result['DeliveryStreamDescription']['DeliveryStreamStatus']
        if status == 'ACTIVE':
            return True
        if status == 'DELETING':
            logging.error('Firehose delivery stream {firehose_name} is being deleted.')
            return False
        time.sleep(5)


def create_firehose_to_s3(firehose_name, s3_bucket_arn, iam_role_name,
                          firehose_src_type='DirectPut',
                          firehose_src_stream=None):

    # Create Firehose-to-S3 IAM role if necessary
    if iam_role_exists(iam_role_name):
        # Retrieve its ARN
        iam_role = get_iam_role_arn(iam_role_name)
    else:
        iam_role = create_iam_role_for_firehose_to_s3(iam_role_name,
                                                      s3_bucket_arn,
                                                      firehose_src_stream)
        if iam_role is None:
            # Error creating IAM role
            return None

    # Create the S3 configuration dictionary
    # Both BucketARN and RoleARN are required
    # Set the buffer interval=60 seconds (Default=300 seconds)
    # Set Prefix and ErrorOutputPrefix for S3 bucket
    s3_config = {
        'BucketARN': s3_bucket_arn,
        'RoleARN': iam_role,
        'Prefix': 'firehosetos3example/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
        'ErrorOutputPrefix': 'firehosetos3erroroutputbase/!{firehose:random-string}/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/',
        'BufferingHints': {
            'IntervalInSeconds': 60,
        },
    }

    # Create the delivery stream
    # By default, the DeliveryStreamType is 'DirectPut'
    firehose_client = boto3.client('firehose')
    try:
        result = firehose_client.create_delivery_stream(DeliveryStreamName=firehose_name,
                DeliveryStreamType=firehose_src_type,
                ExtendedS3DestinationConfiguration=s3_config)
    except ClientError as e:
        logging.error(e)
        return None
    return result['DeliveryStreamARN']

def iam_role_exists(iam_role_name):
    # Retrieve information about the role
    if get_iam_role_arn(iam_role_name) is None:
        return False
    return True


def get_iam_role_arn(iam_role_name):
    
    # Retrieve information about the IAM role
    iam_client = boto3.client('iam')
    try:
        result = iam_client.get_role(RoleName=iam_role_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['Role']['Arn']

def create_iam_role_for_firehose_to_s3(iam_role_name, s3_bucket,firehose_src_stream=None):
    
    # Firehose trusted relationship policy document
    firehose_assume_role = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': '',
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'firehose.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            }
        ]
    }
    iam_client = boto3.client('iam')
    try:
        result = iam_client.create_role(RoleName=iam_role_name,
                                    AssumeRolePolicyDocument=json.dumps(firehose_assume_role))
    except ClientError as e:
        logging.error(e)
        return None
    
    # Wait for IAM Role to be created 
    time.sleep(7)
    
    firehose_role_arn = result['Role']['Arn']

    # Define and attach a policy that grants sufficient S3 permissions
    policy_name = 'firehose_s3_access'
    s3_access = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"{s3_bucket}/*",
                    f"{s3_bucket}"
                ]
            }
        ]
    }
    try:
        iam_client.put_role_policy(RoleName=iam_role_name,
                                   PolicyName=policy_name,
                                   PolicyDocument=json.dumps(s3_access))
    except ClientError as e:
        logging.error(e)
        return None
    
    # Return the ARN of the created IAM role
    return firehose_role_arn

if __name__ == '__main__':
    main()