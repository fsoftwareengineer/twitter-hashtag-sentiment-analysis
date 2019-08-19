# twitter-hashtag-sentiment-analysis
## Summary
Twitter hashtag analysis with AWS kinesis, lambda, and comprehend. This is python code to 
1. create aws infrastructure kinesis firehose - lambda - comprehend stack.
2. start streaming twitter hashtag

## What does this do?
First, it tries to set up AWS infrastructures. 
- creates or gets role arn and update assume role policy and policies to use kinesis, lambda, s3, and cloudwatch. If the role already exists, it updates the policy.
- creates a zip for the lambda function `ProcessingStreamData.py` and upload it to your lambda. If the function already exists, it updates the code.
- creates a S3 bucket to associate with AWS Kinesis firehose. If the bucket exists, it doesn't do anything.
- creates a kinesis firehose. If firehose already exists, it doesn't do anything. 
- Sets up tweeter streamer and starts filering.


## How to run this
#### Prerequisite 
You have to be a twitter developer and has Access and Secret tokens.
Install boto3, tweepy
```
pip install boto3
pip install tweepy
```

#### To run the script
1. Clone the repository.
2. streamer.py
Replace below values with your Twiter API credentials.
```
access_token = "<ACCESS_TOKEN>"
access_token_secret = "<ACCESS_TOKEN_SECRET>"
consumer_key = "<CONSUMER_KEY>"
consumer_secret = "<CONSUMER_SECRET>"
```

3. stream_processor.py
Replace above below to your AWS credentials and config.
If you have a different role name, different firehose name, etc, update them before running. Otherwise it will create new firehose, s3, role, etc.
```
__aws_access_key_id__ = '<AWS_ACCESS_KEY>'
__aws_secret_access_key__ = '<AWS_SECRET_ACCESS_KEY>'

__region__ = '<REGION>'
__account_id__ = '<ACCOUNT_ID>'
```

4. run `python ./app.py <LANGUAGE> <HASHTAG>`
example: `python ./app.py en softwareengineering`


## Contribution
All contributions are welcome.

