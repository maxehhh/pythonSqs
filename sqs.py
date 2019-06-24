#!/usr/bin/python3
import boto3
import json

def toQueue( obj, context ):
    total = 0
    last = 0
    params = {}
    params[last] = {}
    params[last]['Entries'] = []
    params[last]['QueueUrl'] = obj['QueueUrl']

    if len(obj['entries']):
        for index, msg in enumerate(obj['entries']):
            params[last]['Entries'].append({'Id': str(index), 'MessageBody': json.dumps(msg)})
            
            if (index != 0 and (index % 9) == 0):
                last += 1
                params[last] = {}
                params[last]['Entries'] = []
                params[last]['QueueUrl'] = obj['QueueUrl']

        if len(params):
            sqs = boto3.client('sqs')
            
            for i in params:
                response = sqs.send_message_batch( QueueUrl=params[i]['QueueUrl'], Entries=params[i]['Entries'] )
                if len(response) and response['Successful'] and response['ResponseMetadata'] and response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    total += 1
    return total
