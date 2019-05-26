# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import sys
import os
import io
import requests
import json
from queue import Queue, Empty
import cgi
import asyncio
from kademlia.network import Server
import logging
from threading import Thread
from google.cloud import speech
from google.cloud.speech import enums
from google.cloud.speech import types
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import random

import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from http.server import BaseHTTPRequestHandler, HTTPServer
# pylint: disable=E0401
'''
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('kademlia')
log.addHandler(handler)
log.setLevel(logging.DEBUG)
'''
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
SEND_TO_HUB = True
action_loop_time = None
block_blob_service = None
container_name = "masteraudiocontainer"
client = speech.SpeechClient()
TEXT_SUMMARY_ENDPOINT = None
catalogue = None
table_name = "masteraudiotable"
table_partition_key = "fullAudioFiles"
table_service = None
peer_name = "PEER"

def stamp(string):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    string = now + "\t" + string
    return string

work_queue = Queue()
caption_queue = Queue()
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
    temp = Server()
    loop.run_until_complete(temp.listen(5678))
    loop.run_until_complete(temp.bootstrap([(MASTER_IP, 5678)]))
    msg = stamp("{} has joined the P2P network.".format(peer_name))
    send_to_hub(msg)
    print(msg)
    global node
    node = temp
    

# Send a message to IoT Hub
# Route output1 to $upstream in deployment.template.json
def send_to_hub(strMessage):
    if not SEND_TO_HUB:
        return
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
    return response.json()["id"]

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
    msg = stamp("{} has sent a caption request {}.".format(peer_name, audio_file_name))
    send_to_hub(msg)
    print(stamp("Peer's caption request was successful."))
    return True

def add_to_dht(key, value):
    loop.run_until_complete(node.set(key, value))

def try_get_nowait(q):
    job = None
    try:
        job = q.get_nowait()
    except Empty:
        job = None
    return job


async def do_job_async(job):
    request_type = job["request-type"]
    source = job["source"]
    if request_type == "caption":
        audio_file_name = job["audio-file-name"]
        print(stamp("Peer has queued a caption request for audio {} from the Master".format(audio_file_name)))
        t1 = time.monotonic()
        await node.set(audio_file_name, "Dummy Caption Dummy Caption Dummy Caption Dummy Caption.")
        #add_to_dht(audio_file_name, "Dummy Caption Dummy Caption Dummy Caption Dummy Caption.")
        #print("add to dht took {} seconds.".format(time.monotonic() - t1))

def notify_caption_completion(audio_slice_name):
    content = {"request-type" : "caption_completion",
                "source" : SELF_IP,
                "audio-file-name" : audio_slice_name}

    response = requests.post("http://" + MASTER_IP + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Master did not acknowledge peer's completion of job {}.".format(audio_slice_name)))
        return False
    print(stamp("Master acknowledged peer's completion of job {}.".format(audio_slice_name)))
    return True


num_requested = 0
async def action_loop():
    global action_loop_time
    global num_requested
    while True:
        if (num_requested < len(catalogue) and (time.monotonic() - action_loop_time) > 10):
            send_caption_request(catalogue[num_requested])
            action_loop_time = time.monotonic()
            num_requested += 1
        elif caption_queue.qsize() > 0:
            res = try_get_nowait(caption_queue)
            while res:
                audio_file_name, caption = res
                t1 = time.monotonic()
                await node.set(audio_file_name, caption)
                #print("add to dht took {} seconds.".format(time.monotonic() - t1))
                # Tell Master job has been completed
                notify_caption_completion(audio_file_name)
                res = try_get_nowait(caption_queue)
        await asyncio.sleep(0.2)


def send_job_to_google(job):
    temp_audio_file_name = "slice_audio_temp.wav"
    downloads_path = "/app/temp_downloads/"
    audio_file_name = job["audio-file-name"]
    # read the audio file from the blob
    full_path_to_file2 = os.path.join(downloads_path, temp_audio_file_name)
    print("\nDownloading blob to " + full_path_to_file2)
    block_blob_service.get_blob_to_path(container_name, audio_file_name, full_path_to_file2)

    # TODO: send to google
    with io.open(full_path_to_file2, 'rb') as audio_file:
        content = audio_file.read()

    audio = types.RecognitionAudio(content=content)
    config = types.RecognitionConfig(
    encoding=enums.RecognitionConfig.AudioEncoding.LINEAR16,
    sample_rate_hertz=44100,
    language_code='en-US')

    print(stamp("Sent audio slice {} to Google Speech for transcription.".format(audio_file_name)))
    response = client.recognize(config, audio)
    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    print(stamp("Received captions for audio slice {}.".format(audio_file_name)))
    whole_result = ""
    for result in response.results:
        # The first alternative is the most likely one for this portion.
        chosen_result = result.alternatives[0].transcript
        whole_result = whole_result + chosen_result
    msg = stamp("{} has captioned {}: '{}'".format(peer_name, audio_file_name, whole_result))
    send_to_hub(msg)
    print(msg)
    return (audio_file_name, whole_result)

def process_jobs():
    print(stamp('Peer work processor is running on port {}...'.format(PEER_PORT)))
    while 1:
        job = None
        # don't try to work until the server is available
        if (node != None) and work_queue.qsize() > 0:
            job = try_get_nowait(work_queue)
            while job:
                audio_file_name, whole_result = send_job_to_google(job)
                # Get the summary of the captions
                summary = ""
                if (whole_result.strip() != ""):
                    docs = {'documents' : [{'id': audio_file_name, 'language': 'en', 'text': whole_result}]}
                    summary = sendTextForProcessing(docs, TEXT_SUMMARY_ENDPOINT)
                    summary = json.loads(summary)
                    summary = str.join(", ", summary["documents"][0]["keyPhrases"])
                new_whole_result = "{}|{}".format(whole_result, summary)
                res = (audio_file_name, new_whole_result)
                caption_queue.put_nowait(res)
                job = try_get_nowait(work_queue)

def get_catalogue():
    cat = []
    records = table_service.query_entities(table_name, filter="PartitionKey eq '{}'".format(table_partition_key))
    cat = [record.Name for record in records]
    random.shuffle(cat)
    return cat

def main(textSummaryEndpoint):
    print ("Simulated distributed captioning on Azure IoT Edge. Press Ctrl-C to exit.")
    try:
        try:
            global hubManager 
            hubManager = HubManager(PROTOCOL, MESSAGE_TIMEOUT)
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return

        # start request handling thread
        server_thread = Thread(target=run_server)
        server_thread.start()

        # start speech transcription thread
        transcription_thread = Thread(target=process_jobs)
        transcription_thread.start()

        # get the audio catalogue
        global catalogue
        catalogue = get_catalogue()
        print(stamp("Retrieved the audio catalogue."))

        # initialize the loop
        global loop
        loop = asyncio.get_event_loop()
        #loop.set_debug(True)

        print(loop.is_running())

        # Register self with Master.
        id = send_join_request()

        # Join the Kademlia P2P network for the DHT
        peer_kademlia_join()

        print(loop.is_running())


        global action_loop_time
        action_loop_time = time.monotonic()

        loop.create_task(action_loop())

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            node.stop()
            loop.close()

    except KeyboardInterrupt:
        send_leave_request()
        print ( "IoT Edge module sample stopped" )


if __name__ == '__main__':
    TEXT_SUMMARY_ENDPOINT = "http://summarizer:5000/text/analytics/v2.0/keyPhrases"

    try:
        # Retrieve the Master's ip
        MASTER_IP = os.getenv('MASTER', "")
        SELF_IP = os.getenv('SELF', "")
        account_name = os.getenv('LOCAL_STORAGE_ACCOUNT_NAME', "")
        account_key = os.getenv('LOCAL_STORAGE_ACCOUNT_KEY', "")
        global_storage_account_name = os.getenv('GLOBAL_STORAGE_ACCOUNT_NAME', "")
        global_storage_account_key = os.getenv('GLOBAL_STORAGE_ACCOUNT_NAME', "")
    except ValueError as error:
        print (error)
        sys.exit(1)

    if (MASTER_IP and SELF_IP and account_key and account_name and global_storage_account_name and global_storage_account_key) == "":
        print("Error: MASTER or SELF environment variables not found.")
        sys.exit(1)
    peer_name = "PEER@{}".format(SELF_IP)
    connection_string = 'DefaultEndpointsProtocol=https;BlobEndpoint=http://{}:11002/{};AccountName={};AccountKey={};'.format(MASTER_IP, account_name, account_name, account_key)
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, connection_string=connection_string)
    table_connection_string = "DefaultEndpointsProtocol=https;AccountName=audiocaptionstorage;AccountKey=QvgKZ+JpM69MPks1I5x3gN4MTRWn5XbRu40/iAN21AHBVbS+iv9U9Sqnn45zV5GjemHddJGx2/EaZzWBC4Z+ig==;EndpointSuffix=core.windows.net"
    table_service = TableService(account_name=global_storage_account_name, account_key=global_storage_account_key, connection_string=table_connection_string)
    main(TEXT_SUMMARY_ENDPOINT)