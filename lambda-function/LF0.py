import json
import boto3
from datetime import datetime

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

# Create SQS client
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    # print(event)
    msg_from_user = event['messages'][0]['unstructured']['text']

    print(f"Message from frontend: {msg_from_user}")

    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='TWRK2NIWIO', # MODIFY HERE
            botAliasId='HBQALA42FL', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    
    msg_from_lex = response.get('messages', [])
    # request_id = response.get('ResponseMetadata',[])
    msgs = []
    if msg_from_lex:
        current_day = datetime.now()
        for idx in range(len(msg_from_lex)):
            unit_msg = msg_from_lex[idx]
            print(f"Message from Chatbot: {unit_msg['content']}")
            single_msg = {
                'type':'unstructured',
                'unstructured':{
                    'id': str(idx),
                    'text': unit_msg['content'],
                    'timestamp':current_day.strftime("%Y-%m-%d")
                }
            }
            msgs.append(single_msg)
        print(response)
        resp = {
            'statusCode': 200,
            'body': "Hello from LF0!",
            'messages': msgs
        }

        return resp