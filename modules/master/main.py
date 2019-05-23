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
from queue import Queue, Empty
from threading import Thread, Lock
import asyncio
from kademlia.network import Server
from pydub import AudioSegment
import math

import iothub_client
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
# pylint: disable=E0401
worker_number = 1
'''
import logging
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('kademlia')
log.addHandler(handler)
log.setLevel(logging.DEBUG)
'''
current_worker = 0
all_workers = []
all_workers_lock = Lock()

SEND_TO_HUB = True

block_blob_service = None
table_service = None
MASTER_IP = None
MASTER_PORT = 16000
PEER_PORT = 16001
HEADERS = {"Content-Type" : "application/json-patch+json"}
container_name = "masteraudiocontainer"
table_name = "masteraudiotable"
table_partition_key = "fullAudioFiles"
master_name = None

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
MESSAGE_TIMEOUT = 10000

# Choose HTTP, AMQP or MQTT as transport protocol.  
PROTOCOL = IoTHubTransportProvider.MQTT

# global counters
SEND_CALLBACKS = 0

def stamp(string):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    string = now + "\t" + string
    return string

work_queue = Queue()
pending_jobs = {}
#Create custom HTTPRequestHandler class
class MasterServer(BaseHTTPRequestHandler):
    
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
        job = json.loads(self.rfile.read(length).decode("utf-8"))
        work_queue.put_nowait(job)
        response = {}
        response["received"] = "ok"
        if job["request-type"] == "peer_join":
            global worker_number
            response["id"] = worker_number
            all_workers_lock.acquire()
            all_workers.append(job["source"])
            all_workers_lock.release()
            worker_number += 1
        self._set_headers()
        self.wfile.write(bytes(json.dumps(response), "utf-8"))
		
def run_server():
    print(stamp('{} server is starting on port {}...'.format(master_name, MASTER_PORT)))
    server_address = ('', MASTER_PORT)
    httpd = HTTPServer(server_address, MasterServer)
    print(stamp('{} server is running on port {}...'.format(master_name, MASTER_PORT)))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        print(stamp("{} server on port {} has been stopped.".format(master_name, MASTER_PORT)))

def get_completion_status(audio_file_name):
    records = table_service.query_entities(table_name, filter="Name eq '{}'".format(audio_file_name))
    res = -1
    for record in records:
        res = int(record.Status)
    return res

def do_job(job):
    request_type = job["request-type"]
    source = job["source"]
    peer_name = "PEER@{}".format(source)
    if request_type == "peer_join":
        print(stamp("{} has registered the {}".format(master_name, peer_name)))
    elif request_type == "caption_completion":
        audio_slice_name = job["audio-file-name"]
        print(stamp("{} has acknowledged {}'s completion of job {}".format(master_name, peer_name, audio_slice_name)))
        # update pending
        name_split = audio_slice_name.split('_')
        audio_file_name = name_split[0] + ".wav"
        pending_jobs[audio_file_name] -= 1
        # remove from pending if complete
        if pending_jobs[audio_file_name] == 0:
            del pending_jobs[audio_file_name]
            # TODO: add caption result to database
            # update status in database, pending_jobs
            ct = int(name_split[1])
            row_key = name_split[0][5:]
            metadata_item = {"PartitionKey": table_partition_key, "RowKey": row_key, "Name": audio_file_name, "NumChunks": ct, "Status": 1} 
            table_service.update_entity(table_name, metadata_item)
            msg = stamp("Caption request {} has been completed.".format(audio_file_name))
            send_to_hub(msg)
            print(msg)
    elif request_type == "peer_leave":
        print(stamp("{} notes the {}'s intent to leave.".format(master_name, peer_name)))
    elif request_type == "caption":
        audio_file_name = job["audio-file-name"]
        # check if the job is pending
        if audio_file_name in pending_jobs:
            msg = stamp("{} is ignoring caption request {} from {}; job is pending.".format(master_name, audio_file_name, peer_name))
            send_to_hub(msg)
            print(msg)
            return
        # TODO: check if the job is completed in database
        elif (get_completion_status(audio_file_name) == 1):
            msg = stamp("{} is ignoring caption request {} from {}; job is completed.".format(master_name, audio_file_name, peer_name))
            send_to_hub(msg)
            print(msg)
            return

        # otherwise schedule the job
        schedule_caption_job(audio_file_name)
        print(stamp("{} has scheduled a caption request for audio {} from {}".format(master_name, audio_file_name, peer_name)))

def send_caption_job(audio_slice_name):
    global all_workers
    all_workers_lock.acquire()
    if not all_workers:
        print(stamp("{} could not send job {}. No available workers.".format(master_name, audio_slice_name)))
        all_workers_lock.release()
        return

    content = {"request-type" : "caption",
                "source" :"MASTER_IP",
                "audio-file-name" : audio_slice_name}

    global current_worker
    response = requests.post("http://" + all_workers[current_worker] + ":" + str(PEER_PORT), headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's caption request was unsuccessful."))
        all_workers_lock.release()
        return False
    current_worker = (current_worker + 1) % len(all_workers)
    all_workers_lock.release()
    peer_name = "PEER@{}".format(all_workers[current_worker])
    print(stamp("{} sent job {} to {}.".format(master_name, audio_slice_name, peer_name)))
    return True
    

def schedule_caption_job(audio_file_name):
    
    # Download the blob(s).
    # Add '_DOWNLOADED' as prefix to '.txt' so you can see both files in Documents.
    temp_audio_file_name = "full_audio_temp.wav"
    downloads_path = "/app/temp_downloads/"
    full_path_to_file2 = os.path.join(downloads_path, temp_audio_file_name)
    print(stamp("\nDownloading blob to " + full_path_to_file2))
    block_blob_service.get_blob_to_path(container_name, audio_file_name, full_path_to_file2)
    new_audio = AudioSegment.from_wav(full_path_to_file2)
    t1 = 0
    duration = 10 * 1000
    i = 0

    ct = int (math.ceil(float(len(new_audio)) / duration ))

    # update
    # -1 incomplete, 0 pending, 1 complete
    # Mark the job as pending
    name_split = audio_file_name.split('.')
    row_key = name_split[0][5:]
    metadata_item = {"PartitionKey": table_partition_key, "RowKey": row_key, "Name": audio_file_name, "NumChunks": ct, "Status": 0} 
    table_service.update_entity(table_name, metadata_item)
    
    pending_jobs[audio_file_name] = ct
    while (t1 < len(new_audio)):
        t2 = t1 + duration
        temp_audio = new_audio[t1:t2]
        temp_slice_path = os.path.join(downloads_path, 'temp.wav')
        temp_audio.export(temp_slice_path, format="wav")
        just_name = audio_file_name.split('.')[0]
        cloud_name = "{}_{}_{}.wav".format(just_name, ct, i)
        block_blob_service.create_blob_from_path(container_name, cloud_name, temp_slice_path)
        send_caption_job(cloud_name)
        t1 = t2
        i += 1

def try_get_nowait():
    job = None
    try:
        job = work_queue.get_nowait()
    except Empty:
        job = None
    return job

def work():
    while 1:
        job = None
        if work_queue.qsize() > 0:
            job = try_get_nowait()
            while job:
                do_job(job)
                job = try_get_nowait()
        time.sleep(5)

def master_kademlia_join(loop):
    node = Server()
    loop.run_until_complete(node.listen(5678))
    try:
        msg = stamp("{} has joined the P2P network.".format(master_name))
        send_to_hub(msg)
        print(msg)
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        loop.close()
    return node

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
    #print ( "Confirmation received for message with result = %s" % result )
    #print ( "   Total calls confirmed: %d \n" % SEND_CALLBACKS )


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

def main(audio_directory):
    try:
        print ( "Simulated distributed captioning on Azure IoT Edge. Press Ctrl-C to exit." )

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
        audio_files_to_blob_storage(audio_directory)

        # start request handling thread
        server_thread = Thread(target=run_server)
        server_thread.start()

        # start request processing thread
        worker_thread = Thread(target=work)
        worker_thread.start()
        
        # Initiate the Kademlia P2P network for the DHT
        loop = asyncio.get_event_loop()
        #loop.set_debug(True)
        node = master_kademlia_join(loop)
    

    except KeyboardInterrupt:
        print ( "IoT Edge module sample stopped" )

# Upload to blob storage and create table
def audio_files_to_blob_storage(audio_directory):
    print (stamp("{} is uploading audio samples to local blob storage and creating the metadata table...".format(master_name)))
    if (not isdir(audio_directory)):
        print("Error: Invalid directory \"" + audio_directory + "\". Please supply an existing directory.")
        return

    # delete any data from the previous run
    if table_service.exists(table_name):
        print("{} is deleting the old table. Please wait 1 minute...".format(master_name))
        table_service.delete_table(table_name, timeout=60)
        time.sleep(60)

    # create the new table
    table_service.create_table(table_name)

    count = 0
    for file_name in listdir(audio_directory):
        file_path = join(audio_directory, file_name)
        if isfile(file_path) and file_name.endswith(".wav"):
            count += 1
            # store in local blob
            # Upload the created file, use file_name for the blob name
            block_blob_service.create_blob_from_path(container_name, file_name, file_path)

            name_split = file_name.split('.')
            row_key = name_split[0][5:]
            # don't care how many chunks it has unless it's captioned
            metadata_item = {"PartitionKey": table_partition_key, "RowKey": row_key, "Name": file_name, "NumChunks": -1, "Status": -1} 
            table_service.insert_entity(table_name, metadata_item)


    print(stamp("{} uploaded {} files to local blob storage and created the metadata table.".format(master_name, count)))


if __name__ == '__main__':
    try:
        # Retrieve the local storage account name and key from container environment
        MASTER_IP = os.getenv('MASTER', "")
        account_name = os.getenv('LOCAL_STORAGE_ACCOUNT_NAME', "")
        account_key = os.getenv('LOCAL_STORAGE_ACCOUNT_KEY', "")
        global_storage_account_name = os.getenv('GLOBAL_STORAGE_ACCOUNT_NAME', "")
        global_storage_account_key = os.getenv('GLOBAL_STORAGE_ACCOUNT_NAME', "")
    except ValueError as error:
        print (error)
        sys.exit(1)

    if (MASTER_IP and account_name and account_key and global_storage_account_name and global_storage_account_key) == "":
        print("Error: LOCAL_STORAGE_ACCOUNT_NAME and LOCAL_STORAGE_ACCOUNT_KEY environment variables not found.")
        sys.exit(1)
    master_name = "MASTER@{}".format(MASTER_IP)
    connection_string = 'DefaultEndpointsProtocol=https;BlobEndpoint=http://azureblobstorageoniotedge:11002/{};AccountName={};AccountKey={};'.format(account_name, account_name, account_key)
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, connection_string=connection_string)
    table_connection_string = "DefaultEndpointsProtocol=https;AccountName=audiocaptionstorage;AccountKey=QvgKZ+JpM69MPks1I5x3gN4MTRWn5XbRu40/iAN21AHBVbS+iv9U9Sqnn45zV5GjemHddJGx2/EaZzWBC4Z+ig==;EndpointSuffix=core.windows.net"
    table_service = TableService(account_name=global_storage_account_name, account_key=global_storage_account_key, connection_string=table_connection_string)
    audio_directory = "./audio_samples/"
    main(audio_directory)