# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import sys
import os
import requests
import json
from queue import Queue
import cgi

import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from http.server import BaseHTTPRequestHandler, HTTPServer
# pylint: disable=E0401

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
MESSAGE_TIMEOUT = 10000

# Choose HTTP, AMQP or MQTT as transport protocol.  
PROTOCOL = IoTHubTransportProvider.MQTT

# global counters
SEND_CALLBACKS = 0

MASTER_PORT = 16000
PEER_PORT = 16001
MASTER_IP = None
SELF_IP = None
HEADERS = {"Content-Type" : "application/json-patch+json"}

def stamp(string):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    string = now + "\t" + string
    return string

work_queue = Queue()
#Create custom HTTPRequestHandler class
class PeerServer(BaseHTTPRequestHandler):
    
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json-patch+json')
        self.end_headers()
    
   	#handle POST command
    def do_POST(self):
        ctype = self.headers['content-type']

        # only receive json content
        if ctype != "application/json-patch+json":
            self.send_response(400)
            self.end_headers()
            return

        length = int(self.headers['content-length'])
        message = json.loads(self.rfile.read(length).decode("utf-8"))
        work_queue.put_nowait(message)
        response = {}
        response["received"] = "ok"
        self._set_headers()
        self.wfile.write(bytes(json.dumps(response), "utf-8"))

# Send a message to IoT Hub
# Route output1 to $upstream in deployment.template.json
def send_to_hub(strMessage):
    message = IoTHubMessage(bytearray(strMessage, 'utf8'))
    hubManager.send_event_to_output("output1", message, 0)

# Callback received when the message that we send to IoT Hub is processed.
def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    SEND_CALLBACKS += 1
    print ( "Confirmation received for message with result = %s" % result )
    print ( "   Total calls confirmed: %d \n" % SEND_CALLBACKS )


# Send an text to the text summarizing server
# Return the JSON response from the server with the prediction result
def sendTextForProcessing(documents, textSummaryEndpoint):
    #headers = {'Content-Type': 'application/octet-stream'}
    headers = {"Content-Type" : "application/json-patch+json"}

    #with open(imagePath, mode="rb") as test_image:
    try:
        response = requests.post(textSummaryEndpoint, headers = headers, json=documents)
        print("Response from summary service: (" + str(response.status_code) + ") " + json.dumps(response.json()) + "\n")
    except Exception as e:
        print(e)
        print("Response from summary service: (" + str(response.status_code))

    return json.dumps(response.json())

class HubManager(object):
    def __init__(self, protocol, message_timeout):
        self.client_protocol = protocol
        self.client = IoTHubModuleClient()
        self.client.create_from_environment(protocol)
        # set the time until a message times out
        self.client.set_option("messageTimeout", message_timeout)

    # Sends a message to an output queue, to be routed by IoT Edge hub. 
    def send_event_to_output(self, outputQueueName, event, send_context):
        self.client.send_event_async(
            outputQueueName, event, send_confirmation_callback, send_context)

def send_join_request():
    content = {"request-type" : "peer_join",
                "source" : SELF_IP}
    response = requests.post("http://" + MASTER_IP + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's join request was unsuccessful."))
        return False
    print(stamp("Peer's join request was successful."))
    return True

def send_leave_request():
    content = {"request-type" : "peer_leave",
                "source" : SELF_IP}
    response = requests.post("http://" + MASTER_IP + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's leave request was unsuccessful."))
        return False
    print(stamp("Peer's leave request was successful."))
    return True

def send_caption_request():
    content = {"request-type" : "caption",
                "source" : SELF_IP,
                "audio-file-name" : "fake_news.wav"}
    # Upload the blob here

    response = requests.post("http://" + MASTER_IP + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's caption request was unsuccessful."))
        return False
    print(stamp("Peer's caption request was successful."))
    return True


def main(documents, textSummaryEndpoint):
    print ("Simulated distributed captioning on Azure IoT Edge. Press Ctrl-C to exit.")
    try:
        try:
            global hubManager 
            hubManager = HubManager(PROTOCOL, MESSAGE_TIMEOUT)
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return

        # Register self with Master.
        send_join_request()

        send_caption_request()

        # Get the summary for the predicted captions
        while True:
            summary = sendTextForProcessing(documents, textSummaryEndpoint)
            print(stamp("Got summary for captions."))
            send_to_hub(summary)
            time.sleep(10)

    except KeyboardInterrupt:
        send_leave_request()
        print ( "IoT Edge module sample stopped" )

def test_connection():
    content1 = {"request-type" : "peer_join",
            "source" : "10.16.87.123",
            "audio-file-name" : ""}
    content2 = {"request-type" : "peer_leave",
            "source" : "10.16.87.123",
            "audio-file-name" : ""}
    content3 = {"request-type" : "caption",
            "source" : "10.16.87.123",
            "audio-file-name" : "fake_news.wav"}
    headers = {"Content-Type" : "application/json-patch+json"}

    while 1:
        response = requests.post("http://" + MASTER_IP + ":16000", headers = headers, json = content1)
        print(json.dumps(response.json()))
        time.sleep(10)
        response = requests.post("http://" + MASTER_IP + ":16000", headers = headers, json = content2)
        print(json.dumps(response.json()))
        time.sleep(10)
        response = requests.post("http://" + MASTER_IP + ":16000", headers = headers, json = content3)
        print(json.dumps(response.json()))
        time.sleep(10)

if __name__ == '__main__':
    TEXT_SUMMARY_ENDPOINT = "http://summarizer:5000/text/analytics/v2.0/keyPhrases"
    try:
        # Retrieve the Master's ip
        MASTER_IP = os.getenv('MASTER', "")
        SELF_IP = os.getenv('SELF', "")
    except ValueError as error:
        print (error)
        sys.exit(1)

    if (not MASTER_IP) or (not SELF_IP):
        print("Error: MASTER or SELF environment variables not found.")
        sys.exit(1)

    documents = {'documents' : [
        {'id': '1', 'language': 'en', 'text': 'I had a wonderful experience! The rooms were wonderful and the staff was helpful.'},
        {'id': '2', 'language': 'en', 'text': 'I had a terrible time at the hotel. The staff was rude and the food was awful.'},  
        {'id': '3', 'language': 'es', 'text': 'Los caminos que llevan hasta Monte Rainier son espectaculares y hermosos.'},  
        {'id': '4', 'language': 'es', 'text': 'La carretera estaba atascada. Había mucho tráfico el día de ayer.'}
        ]}
    main(documents, TEXT_SUMMARY_ENDPOINT)