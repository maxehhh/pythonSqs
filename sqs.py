#!/usr/bin/python3
import boto3
import json
from botocore.exceptions import ClientError

# Push in batch 10 messages
def toBatch( dic, context ):
    total = 0
    last = 0
    params = {
        last: {
            "Entries": [],
            "QueueUrl": dic["QueueUrl"]
        }
    }

    if len(dic["Entries"]):
        attributes = {}

        if len(dic["Entries"]["MessageAttributes"]):
            for index, key in enumerate(dic["Entries"]["MessageAttributes"]):
                attributes[key] = {
                    "StringValue": dic["Entries"]["MessageAttributes"][key],
                    "DataType": "String"
                }

        for i, entry in enumerate(dic["Entries"]["MessageBody"]):
            if len(attributes):
                params[last]["Entries"].append({ "Id": str(i), "MessageBody": json.dumps(entry), "MessageAttributes": attributes })
            else:
                params[last]["Entries"].append({ "Id": str(i), "MessageBody": json.dumps(entry) })
            
            if (i != 0 and (i % 9) == 0):
                last += 1
                params[last] = {
                    "Entries": [],
                    "QueueUrl": dic["QueueUrl"]
                }

        if len(params):
            sqs = boto3.client("sqs")
            
            for idx in params:
                response = sqs.send_message_batch( QueueUrl=params[idx]["QueueUrl"], Entries=params[idx]["Entries"] )
                if len(response) and response["Successful"] and response["ResponseMetadata"] and response["ResponseMetadata"]['HTTPStatusCode'] == 200:
                    total += 1
    return total

# Single message push
def toQueue( obj, context ): return False

def getQueue( obj, context ):
    try:
        sqs = boto3.client('sqs')
        queue = sqs.get_queue_url(
            QueueName=obj['url'],
            QueueOwnerAWSAccountId=obj['account']
        )
        return { 'Ok': True, 'Error': False }
    except ClientError as e:
        return { 'Ok': False, 'Error': True, 'Mensaje': e.response['Error']['Message'], 'Codigo': e.response['ResponseMetadata']['HTTPStatusCode'] }

def createQueue( obj, context ): return False
