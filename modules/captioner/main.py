# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import sys
import os
import requests
import json
from queue import Queue, Empty
import cgi
import asyncio
from kademlia.network import Server
import logging
from threading import Thread

import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from http.server import BaseHTTPRequestHandler, HTTPServer
# pylint: disable=E0401

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('kademlia')
log.addHandler(handler)
log.setLevel(logging.DEBUG)

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
loop = None
node = None

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

def run_server():
    print(stamp('Peer server is starting on port {}...'.format(PEER_PORT)))
    server_address = ('', PEER_PORT)
    httpd = HTTPServer(server_address, PeerServer)
    print(stamp('Peer server is running on port {}...'.format(PEER_PORT)))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        print(stamp("Peer server on port {} has been stopped.".format(PEER_PORT)))

def peer_kademlia_join():
    node = Server()
    loop.run_until_complete(node.listen(5678))
    loop.run_until_complete(node.bootstrap([(MASTER_IP, 5678)]))
    time.sleep(7)
    print(stamp("Peer has joined the Kademlia P2P network for the DHT."))
    return node

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

def send_caption_request(audio_file_name):
    content = {"request-type" : "caption",
                "source" : SELF_IP,
                "audio-file-name" : audio_file_name}
    # Upload the blob here

    response = requests.post("http://" + MASTER_IP + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's caption request was unsuccessful."))
        return False
    print(stamp("Peer's caption request was successful."))
    return True

def add_to_dht(key, value):
    loop.run_until_complete(node.set(key, value))

def try_get_nowait():
    job = None
    try:
        job = work_queue.get_nowait()
    except Empty:
        job = None
    return job

def do_job(job):
    request_type = job["request-type"]
    source = job["source"]
    if request_type == "caption":
        audio_file_name = job["audio-file-name"]
        print(stamp("Peer has queued a caption request for audio {} from the Master".format(audio_file_name)))
        add_to_dht(audio_file_name, "Dummy Caption Dummy Caption Dummy Caption Dummy Caption.")


def work(lp):
    asyncio.set_event_loop(lp)
    print(stamp('Peer work processor is running on port {}...'.format(PEER_PORT)))
    while 1:
        job = None
        if work_queue.qsize() > 0:
            job = try_get_nowait()
            while job:
                do_job(job)
                job = try_get_nowait()
        time.sleep(5)

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

        # Join the Kademlia P2P network for the DHT
        global loop
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        global node
        node = peer_kademlia_join()

        #give the node some time to join the network
        time.sleep(5)

        # start request handling thread
        server_thread = Thread(target=run_server)
        server_thread.start()

        # start request processing thread
        worker_thread = Thread(target=work, args=(loop,))
        worker_thread.start()

        while True:
            send_caption_request("audio4.wav")
            time.sleep(10)


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