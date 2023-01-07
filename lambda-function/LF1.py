import boto3
from datetime import datetime
from datetime import timedelta

import json
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

# Create SQS client
sqs = boto3.client('sqs')

def dining_suggestions(intent_request):
    state = intent_request['sessionState']
    # logger.debug(state)
    slots = state['intent']['slots']
    name = "DiningSuggestionsIntent"
    rst_msgs,valid = validate_slot(slots)
    if not valid:
        state = 'InProgress'
        return_msg = [{
          "content": "Your input is not valid in the following fields, please examine and REDO the request",
          "contentType": "PlainText"
        }]
        for msg in rst_msgs:
            return_msg.append(msg)
        return elicit_slot(return_msg, state, name)
    # logger.debug(slots)
    send_sqs(slots)
    # intent_request['sessionState']['intent']['state'] = "Fulfilled"
    return_msg = {
      "content": "You’re all set. Expect my suggestions via Email or SMS Messages shortly! Have a good day.",
      "contentType": "PlainText"
    }
    state = "Fulfilled"
    
    return close(return_msg, state, name)

def validate_slot(slots):
    date = ''
    time = ''
    false_msg = []
    for key in slots:
        value = slots[key]['value']['resolvedValues'][0]
        if key == 'NumberOfPeople':
            if int(value)<1:
                false_msg.append({"content": "Your party number should larger than 0.",
                                "contentType": "PlainText"})
        if key == 'DiningDate':
            date = value
        if key == 'DiningTime':
            time = value
    date_time = date + ' ' + time
    datetime_object = datetime.strptime(date_time, '%Y-%m-%d %H:%M')
    logger.debug(datetime_object)
    now = datetime.now() - timedelta(hours=4)
    logger.debug(now)
    if now>datetime_object:
        logger.debug('now>datetime_object')
        false_msg.append({"content": "Your dining time should be in the future.",
                                "contentType": "PlainText"})
    logger.debug(false_msg)
    if len(false_msg)>0:
        return false_msg, False
    else:
        return false_msg, True

def elicit_slot(return_msg, state, name):
    resp = {
        "messages": return_msg,
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": "Location"
            },
            "intent": {
                "name": name,
                "slots": {},
                "state": state,
                "confirmationState": "None"
            },
        }
    }
    return resp
    
def close(return_msg, state, name):
    resp = {
        "messages": [return_msg],
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": name,
                "slots": {},
                "state": state,
                "confirmationState": "None"
            },
        }
    }
    return resp
    
def send_sqs(slots):
    msg_to_sqs = {}
    logger.debug(slots)
    for key in slots:
        value = slots[key]['value']['resolvedValues'][0]
        if value != 'Persian':
            value = value.lower()
        msg_to_sqs[key] = value
    resp_sqs = sqs.send_message(
        QueueUrl = "https://sqs.us-east-1.amazonaws.com/019820692062/Q1",
        MessageBody = json.dumps(msg_to_sqs)
    )
    
    
def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    state = intent_request['sessionState']
    
    logger.debug(intent_request)
    if intent_name == 'DiningSuggestionsIntent':
        resp = dining_suggestions(intent_request)
        logger.debug(resp)
        return resp


def lambda_handler(event, context):
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)



