# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import sys
import os
from os import listdir
from os.path import isfile, join, isdir
import requests
import json
import io

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