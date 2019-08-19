import sys
import boto3
from botocore.exceptions import ClientError
import json
import logging
import time
import shutil

__processor_name__ = 'firehose'
__storage_location__ = 's3'
__aws_access_key_id__ = '<AWS_ACCESS_KEY>'
__aws_secret_access_key__ = '<AWS_SECRET_ACCESS_KEY>'

__iam_role_name__ = 'streamer_dev'
__lambda_function_name__ = 'ProcessStreamData'
__region__ = '<REGION>'
__account_id__ = '<ACCOUNT_ID>'

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
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

stack_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "comprehend:*",
                "s3:ListAllMyBuckets",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "iam:ListRoles",
                "iam:GetRole"
            ],
            "Effect": "Allow",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "es:ESHttpPost"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:" + __region__ + ":" + __account_id__ + ":*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:" + __region__ + ":" + __account_id__ + ":log-group:/aws/lambda/" + __lambda_function_name__ + ":*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:GetFunctionConfiguration"
            ],
            "Resource": "arn:aws:lambda:" + __region__ + ":" + __account_id__ + ":function:" + __lambda_function_name__ + ":*"
        }
    ]
}


class StreamProcessor:
    stream_name = ""
    storage_name = ""
    iam = boto3.client(
        'iam',
        aws_access_key_id=__aws_access_key_id__,
        aws_secret_access_key=__aws_secret_access_key__)

    firehose = boto3.client(
        __processor_name__,
        aws_access_key_id=__aws_access_key_id__,
        aws_secret_access_key=__aws_secret_access_key__)

    s3 = boto3.client(
        __storage_location__,
        aws_access_key_id=__aws_access_key_id__,
        aws_secret_access_key=__aws_secret_access_key__)

    def __init__(self, stream_name, storage_name):
        self.stream_name = stream_name
        self.storage_name = storage_name

    def create_processor(self):
        bucket_name = self.storage_name + '-' + self.stream_name
        response = self.__create_s3_bucket(bucket_name)
        print(response)
        bucket_arn = 'arn:aws:s3:::' + bucket_name
        print("Using iam role : " + __iam_role_name__)

        # upsert lambda function

        self.__create_firehose_to_s3(firehose_name=self.stream_name, s3_bucket_arn=bucket_arn,
                                     iam_role_name=__iam_role_name__, firehose_src_type='DirectPut',
                                     firehose_src_stream=None)
        self.__wait_for_active_firehose(self.stream_name)
        print("AWS firehose streaming engine is ACTIVE.")

    def put_record(self, data):
        response = self.firehose.put_record(
            DeliveryStreamName=self.stream_name,
            Record={
                'Data': data
            }
        )
        print('put record : ' + str(response))

    def __zip_file(self):
        import zipfile
        with zipfile.ZipFile(__lambda_function_name__ + ".zip", 'w') as z:
            z.write(__lambda_function_name__ + ".py")

        # Loads the zip file as binary code.
        with open(__lambda_function_name__ + ".zip", 'rb') as f:
            code = f.read()
        return code

    def __upsert_lambda(self, role_arn):
        code = self.__zip_file()

        lambda_client = boto3.client('lambda')
        fn_name = __lambda_function_name__
        fn_role = role_arn
        try:
            lambda_client.create_function(
                FunctionName=fn_name,
                Runtime='python3.7',
                Role=fn_role,
                Handler=fn_name + ".lambda_handler",
                Code={'ZipFile': code}
            )
        except ClientError as e:
            print("Error creating... " + str(e.response))
            print("Try updating..")
            lambda_client.update_function_code(
                FunctionName=fn_name,
                ZipFile=code
            )
        return fn_name

    # client.create_delivery_stream()
    def __create_s3_bucket(self, bucket_name):
        print("Creating a bucket... " + bucket_name)
        try:
            response = self.s3.create_bucket(
                ACL='private',
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': __region__
                }
            )
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print("Bucket already exists. skipping..")
            else:
                print("Unknown error, exit..")
                sys.exit()

    def __create_iam_role_for_firehose_to_s3(self, iam_role_name, s3_bucket):
        print("Creating iam role for firehose to s3...")

        """Create an IAM role for a Firehose delivery system to S3

        :param iam_role_name: Name of IAM role
        :param s3_bucket: ARN of S3 bucket
        :return: ARN of IAM role. If error, returns None.
        """
        iam_client = self.iam
        firehose_role_arn = None
        try:
            result = iam_client.create_role(RoleName=iam_role_name,
                                            AssumeRolePolicyDocument=json.dumps(firehose_assume_role))
            firehose_role_arn = result['Role']['Arn']
        except ClientError as e:
            print("error " + str(e))

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
            print("Error putting role policy")
            return None
        return firehose_role_arn

    def __get_iam_role_arn(self, iam_role_name):
        """Retrieve the ARN of the specified IAM role

        :param iam_role_name: IAM role name
        :return: If the IAM role exists, return ARN, else None
        """

        # Try to retrieve information about the role
        iam_client = self.iam

        # Define and attach a policy that grants sufficient S3 permissions
        policy_name = 'stack_policy'

        try:
            result = iam_client.update_assume_role_policy(RoleName=iam_role_name,
                                                          PolicyDocument=json.dumps(firehose_assume_role))
            iam_client.put_role_policy(RoleName=iam_role_name,
                                       PolicyName=policy_name,
                                       PolicyDocument=json.dumps(stack_policy))
            result = iam_client.get_role(RoleName=iam_role_name)
        except ClientError as e:
            print("Error " + str(e.response))
            exit(1)
        return result['Role']['Arn']

    def __iam_role_exists(self, iam_role_name):
        # Try to retrieve information about the role
        if self.__get_iam_role_arn(iam_role_name) is None:
            return False
        return True

    def __create_firehose_to_s3(self,
                                firehose_name,
                                s3_bucket_arn,
                                iam_role_name,
                                firehose_src_type='DirectPut',
                                firehose_src_stream=None):
        print("Creating firehose to s3...")
        # Create Firehose-to-S3 IAM role if necessary
        if self.__iam_role_exists(iam_role_name):
            # Retrieve its ARN
            iam_role = self.__get_iam_role_arn(iam_role_name)
        else:
            iam_role = self.__create_iam_role_for_firehose_to_s3(iam_role_name,
                                                                 s3_bucket_arn)
            if iam_role is None:
                # Error creating IAM role
                return None

        print("upsert lambda function before configuring data transformation with role : " + iam_role)
        function_name = self.__upsert_lambda(iam_role)
        # Create the S3 configuration dictionary
        # Both BucketARN and RoleARN are required
        # Set the buffer interval=60 seconds (Default=300 seconds)
        s3_config = {
            'BucketARN': s3_bucket_arn,
            'RoleARN': iam_role,
            'BufferingHints': {
                'IntervalInSeconds': 60,
            },
            'ProcessingConfiguration': {
                'Enabled': True,
                'Processors': [
                    {
                        'Type': 'Lambda',
                        "Parameters": [
                            {
                                "ParameterName": "LambdaArn",
                                "ParameterValue": "arn:aws:lambda:" + __region__ + ":" + __account_id__ + ":function:" + function_name
                                # change this to dynamic
                            },
                            {
                                "ParameterName": "NumberOfRetries",
                                "ParameterValue": "1"
                            },
                            {
                                "ParameterName": "RoleArn",
                                "ParameterValue": iam_role
                            },
                            {
                                "ParameterName": "BufferSizeInMBs",
                                "ParameterValue": "3"
                            },
                            {
                                "ParameterName": "BufferIntervalInSeconds",
                                "ParameterValue": "60"
                            }
                        ]
                    }
                ]
            }
        }

        # Create the delivery stream
        # By default, the DeliveryStreamType='DirectPut'
        firehose_client = self.firehose
        try:
            if firehose_src_type == 'KinesisStreamAsSource':
                # Define the Kinesis Data Stream configuration
                stream_config = {
                    'KinesisStreamARN': firehose_src_stream,
                    'RoleARN': iam_role,
                }
                result = firehose_client.create_delivery_stream(
                    DeliveryStreamName=firehose_name,
                    DeliveryStreamType=firehose_src_type,
                    KinesisStreamSourceConfiguration=stream_config,
                    ExtendedS3DestinationConfiguration=s3_config)
            else:
                result = firehose_client.create_delivery_stream(
                    DeliveryStreamName=firehose_name,
                    DeliveryStreamType=firehose_src_type,
                    ExtendedS3DestinationConfiguration=s3_config)
        except ClientError as e:
            print("Error, " + str(e))
            return None
        return result['DeliveryStreamARN']

    def __wait_for_active_firehose(self, firehose_name):
        """Wait until the Firehose delivery stream is active

        :param firehose_name: Name of Firehose delivery stream
        :return: True if delivery stream is active. Otherwise, False.
        """

        # Wait until the stream is active
        firehose_client = self.firehose
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
                logging.error(f'Firehose delivery stream {firehose_name} is being deleted.')
                return False
            time.sleep(2)
