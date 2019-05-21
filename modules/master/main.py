# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
from datetime import datetime
import sys
import os
from os import listdir
from os.path import isfile, join, isdir
import requests
import json
import io
from http.server import BaseHTTPRequestHandler, HTTPServer
import cgi
from queue import Queue
from threading import Thread

import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from azure.storage.blob import BlockBlobService, PublicAccess
# pylint: disable=E0401

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
MESSAGE_TIMEOUT = 10000

# Choose HTTP, AMQP or MQTT as transport protocol.  
PROTOCOL = IoTHubTransportProvider.MQTT

# global counters
SEND_CALLBACKS = 0

def stamp(string):
    now = datetime.now()
    string = str(now) + ": " + string
    return string


work_queue = Queue()
#Create custom HTTPRequestHandler class
class MasterServer(BaseHTTPRequestHandler):
    
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json-patch+json')
        self.end_headers()
    
   	#handle POST command
    def do_POST(self):
        print(self.headers.items())
        ctype = self.headers['content-type']

        # only receive json content
        if ctype != "application/json-patch+json":
            self.send_response(400)
            self.end_headers()
            return

        length = int(self.headers['content-length'])
        job = json.loads(self.rfile.read(length).decode("utf-8"))
        job["host"] = self.headers["host"]
        work_queue.put_nowait(job)
        response = {}
        response["received"] = "ok"
        self._set_headers()
        self.wfile.write(bytes(json.dumps(response), "utf-8"))
		
def run():
    port = 16000
    print(stamp('Master server is starting on port {}...'.format(port)))
    server_address = ('', 16000)
    httpd = HTTPServer(server_address, MasterServer)
    print(stamp('Master server is running on port {}...'.format(port)))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        print(stamp("Master server on port {} has been stopped.".format(port)))

def do_job(job):
    request_type = job["request-type"]
    host = job["host"]
    if request_type == "peer_join":
        print(stamp("Master has registered the device at {}".format(host)))
    elif request_type == "peer_leave":
        print(stamp("Master notes the device at {}'s intent to leave.".format(host)))
    elif request_type == "caption":
        audio_file_name = job["audio-file-name"]
        print(stamp("Master has queued a caption request for audio {} from the device at {}".format(audio_file_name, host)))

def work():
    while 1:
        job = None
        if work_queue.qsize() > 0:
            job = work_queue.get_nowait()
        if job:
            do_job(job)
        time.sleep(5)

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

def main(block_blob_service, container_name, audio_directory):
    try:
        print ( "Simulated text summary Azure IoT Edge. Press Ctrl-C to exit." )

        try:
            global hubManager 
            hubManager = HubManager(PROTOCOL, MESSAGE_TIMEOUT)
        except IoTHubError as iothub_error:
            print ( "Unexpected error %s from IoTHub" % iothub_error )
            return
        
        # create container and set access to public
        block_blob_service.create_container(container_name)
        block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)

        #upload audio samples to directory
        audio_files_to_blob_storage(block_blob_service, container_name, audio_directory)

        # start request handling thread
        server_thread = Thread(target=run)
        server_thread.start()
        # start request processing thread
        worker_thread = Thread(target=work)
        worker_thread.start()

        server_thread.join()
        worker_thread.join()
        
        while True:
            #send_to_hub(classification)
            time.sleep(10)
    

    except KeyboardInterrupt:
        print ( "IoT Edge module sample stopped" )


def audio_files_to_blob_storage(block_blob_service, container_name, audio_directory):
    print ( "Uploading audio samples to local blob storage.")
    if (not isdir(audio_directory)):
        print("Error: Invalid directory \"" + audio_directory + "\". Please supply an existing directory.")
        return

    count = 0
    for file_name in listdir(audio_directory):
        file_path = join(audio_directory, file_name)
        if isfile(file_path) and file_name.endswith(".wav"):
            count += 1
            # store in local blob
            # Upload the created file, use file_name for the blob name
            block_blob_service.create_blob_from_path(container_name, file_name, file_path)
            '''
            with io.open(audio_path, 'rb') as audio_file:
                content = audio_file.read()
            '''

    print("Uploaded", count, "files to local blob storage as blobs.")


if __name__ == '__main__':
    try:
        # Retrieve the local storage account name and key from container environment
        account_name = os.getenv('LOCAL_STORAGE_ACCOUNT_NAME', "")
        account_key = os.getenv('LOCAL_STORAGE_ACCOUNT_KEY', "")
        print("account_name,account_key", account_name, account_key)
    except ValueError as error:
        print ( error )
        sys.exit(1)

    if (not account_name) or (not account_key):
        print("Error: LOCAL_STORAGE_ACCOUNT_NAME and LOCAL_STORAGE_ACCOUNT_KEY environment variables not found.")
        sys.exit(1)

    connection_string = 'DefaultEndpointsProtocol=https;BlobEndpoint=http://azureblobstorageoniotedge:11002/{};AccountName={};AccountKey={};'.format(account_name, account_name, account_key)
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, connection_string=connection_string)
    container_name = "masteraudiocontainer"
    audio_directory = "./audio_samples/"
    main(block_blob_service, container_name, audio_directory)