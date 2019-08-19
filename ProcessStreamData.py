from __future__ import print_function

import base64
import json
import boto3

print('Loading function')
__region__ = '<REGION>'
comprehend = boto3.client(service_name='comprehend', region_name=__region__)


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        decoded_payload = base64.b64decode(record['data'])
        print("decoded_payload " + str(decoded_payload))
        payload = decoded_payload.decode("utf-8")

        print('Calling DetectSentiment')
        print(json.dumps(comprehend.detect_sentiment(Text=payload, LanguageCode='en'), sort_keys=True, indent=4))
        print('End of DetectSentiment\n')

        # Do custom processing on the payload here
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(payload).encode('utf-8') + b'\n').decode('utf-8')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': output}
