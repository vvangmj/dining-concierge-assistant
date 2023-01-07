import json
import ast
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection,AWSV4SignerAuth
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import requests
import random

# Reference: https://docs.aws.amazon.com/ses/latest/dg/send-email-raw.html


region = 'us-east-1'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')
sqs_client = boto3.client('sqs')
sms_client = boto3.client('sns')
queue_url = 'https://sqs.us-east-1.amazonaws.com/019820692062/Q1'
host = 'search-restaurants-6vnek5tpsoommj6acyiww2vdue.us-east-1.es.amazonaws.com'

SUBJECT = "Your Dining Suggestions"
SENDER = "wangmnjn@gmail.com"
ses_client = boto3.client('ses',region_name="us-east-1")


es = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    use_ssl=True,
    verify_certs=True,
    http_auth=auth,
    connection_class=RequestsHttpConnection
)

def poll_sqs():
    raw_msg = sqs_client.receive_message(
        QueueUrl = queue_url, 
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=5,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
    )
    # processed = set()
    sqs_msg = {}
    
    if 'Messages' in raw_msg:
        for message in raw_msg['Messages']:
            print(message)
            receipt_handle = message['ReceiptHandle']
            print("receipt_handle:", receipt_handle)
            
            if receipt_handle in sqs_msg:
                continue
            
            info = ast.literal_eval(message['Body'])
            print("info: ",info)
            
            delete_sqs(receipt_handle)
            sqs_msg[receipt_handle] = info
            # processed.add(receipt_handle)
            
    return sqs_msg
    
def delete_sqs(receipt_handle):
    sqs_client.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )

def search_es(msg):
    headers = {"Content-Type":"application/json"}
    cuisine = msg['Cuisine']
    
    es_query = {
        "size": 100,
        "query": {
            "query_string": {
                "default_field": "Cuisine",
                "query": cuisine
            }
        }
    }
    
    r = es.search(body = es_query, index = "restaurants")
    print('r:', r)
    resp = r['hits']['hits']
    return resp
    
    
def search_db(sqs_msg, resp):
    db_rst = []
    restaurant_name = set()
    for rec in resp:
        businessID = rec["_source"]["RestaurantID"]
        item = table.get_item(Key={"Business ID": businessID})
        if ('Item' not in item) or (item['Item']['Name'] in restaurant_name):
            continue
        db_rst.append(item['Item'])
        restaurant_name.add(item['Item']['Name'])
        if(len(db_rst)==10):
            break
    print(db_rst)    
    random.shuffle(db_rst)
    msg_begin = 'Hello! Here are my {} restaurant suggestions for {} people, for {} at {}:\n'.format(sqs_msg['Cuisine'], sqs_msg['NumberOfPeople'], sqs_msg['DiningDate'], sqs_msg['DiningTime'])
    suggest1 = '1. {}, located at {}\n'.format(db_rst[0]['Name'], db_rst[0]['Address'])
    suggest2 = '2. {}, located at {}\n'.format(db_rst[1]['Name'], db_rst[1]['Address'])
    suggest3 = '3. {}, located at {}\n'.format(db_rst[2]['Name'], db_rst[2]['Address'])
    msg_end = 'Enjoy your meal!\n'
    fin_msg = msg_begin + suggest1 + suggest2 + suggest3 + msg_end
    print("SMS message:", fin_msg)
    return fin_msg

def send_sns(phone_num, msg):
    resp = sms_client.publish(
        PhoneNumber = phone_num,
        Message = msg
    )

def send_email(email, msg):
    RECIPIENT = email
    BODY_TEXT = msg

    msg = MIMEMultipart('mixed')
    msg['Subject'] = SUBJECT 
    msg['From'] = SENDER 
    msg['To'] = RECIPIENT
    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(BODY_TEXT.encode("utf-8"), 'plain', "utf-8")
    msg_body.attach(textpart)
    msg.attach(msg_body)

    try:
        response = ses_client.send_raw_email(
            Source=SENDER,
            Destinations=[
                RECIPIENT
            ],
            RawMessage={
                'Data':msg.as_string(),
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):
    print('LF2 triggered.')
    sqs_msg = poll_sqs()
    
    print('------Poll Info From ElasticSearch------')
    for key in sqs_msg:
        print('sqs msg:',sqs_msg[key])
        es_resp = search_es(sqs_msg[key])
        fin_msg = search_db(sqs_msg[key], es_resp)
        email = sqs_msg[key]['Email']
        # phone = sqs_msg[key]['Phone']
        send_email(email, fin_msg)
        # send_sns(phone, fin_msg)

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from LF2!')
    }
