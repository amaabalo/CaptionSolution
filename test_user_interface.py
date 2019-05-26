from tkinter import *
import tkinter as tk
import time
import asyncio
import sys
from azure.storage.blob import BlockBlobService
from pydub import AudioSegment
from kademlia.network import Server
import math
import requests
from queue import Queue, Empty
from http.server import BaseHTTPRequestHandler, HTTPServer
import asyncio
import pygame
from threading import Thread



if len(sys.argv) != 5:
    print("Usage: python3 {} <bootstrap node> <bootstrap port> <audio file name> <mode>".format(sys.argv[0]))
    sys.exit(1)

work_queue = Queue()
caption_queue = Queue()
PEER_PORT = 16001
SELF_IP = requests.get('https://api.ipify.org').text
peer_name = "PEER@{}".format(SELF_IP)
master_ip = sys.argv[1]
port = int(sys.argv[2])
mode = sys.argv[4]
container_name = "masteraudiocontainer"
account_name = "masterlocalstorage"
account_key = "OkcVfp37Fqt5h9H5AoKLDXxMAOe1E3TF7d7HS7nWTYHOyn5DLw4YC0aRLSEMUOsTXJTvrH2nXM4xquls7Bs/pQ=="
connection_string = 'DefaultEndpointsProtocol=https;BlobEndpoint=http://{}:11002/{};AccountName={};AccountKey={};'.format(master_ip, account_name, account_name, account_key)
audio_file_name = sys.argv[3] 
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, connection_string=connection_string)
node = None
loop = None
HEADERS = {"Content-Type" : "application/json-patch+json"}

def stamp(string):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    string = now + "\t" + string
    return string

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
		work_queue.append(message)
		response = {}
		response["received"] = "ok"
		self._set_headers()
		self.wfile.write(bytes(json.dumps(response), "utf-8"))

def send_caption_request(audio_file_name):
	content = {"request-type" : "caption",
				"source" : SELF_IP,
				"audio-file-name" : audio_file_name}
	# Upload the blob here

	response = requests.post("http://" + master_ip + ":16000", headers = HEADERS, json = content)
	if (response.status_code != 200):
		print (stamp("Peer's caption request was unsuccessful."))
		return False
	msg = stamp("{} has sent a caption request {}.".format(peer_name, audio_file_name))
	print(stamp("Peer's caption request was successful."))
	return True

def send_join_request():
    content = {"request-type" : "peer_join",
                "source" : SELF_IP}
    response = requests.post("http://" + master_ip + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Peer's join request was unsuccessful."))
        return False
    print(stamp("Peer's join request was successful."))
    return response.json()["id"]

# play the audio, enter the caption, enter keyphrases, send response, place in queue to be sent, run on new thread
def process_jobs():
	print(stamp('Peer work processor is running on port {}...'.format(PEER_PORT)))
	while 1:
		job = None
		# don't try to work until the server is available
		if (node != None) and work_queue.qsize() > 0:
			job = try_get_nowait(work_queue)
			while job:
				# play the snippet
				audio_file_name = job["audio_file_name"]
				print("Downloading and playing new job {}".format(audio_file_name))
				block_blob_service.get_blob_to_path(container_name, audio_file_name, audio_file_name)
				pygame.mixer.music.load(audio_file_name)
				pygame.mixer.music.play()

				# Get the caption
				captions = input("Please input captions for this audio: ")
				captions = captions.strip()

                # Get the key words of the captions
				summary = input("Please input some key phrases: ")
				summary = summary.strip()

				# place in queue to be added to dht
				new_whole_result = "{}|{}".format(caption, summary)
				res = (audio_file_name, new_whole_result)
				caption_queue.put_nowait(res)
				job = try_get_nowait(work_queue)
		else:
			time.sleep(1)

def peer_kademlia_join():
	temp = Server()
	loop.run_until_complete(temp.listen(5678))
	loop.run_until_complete(temp.bootstrap([(master_ip, 5678)]))
	msg = stamp("{} has joined the P2P network.".format(peer_name))
	print(msg)
	global node
	node = temp

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

def try_get_nowait(q):
    job = None
    try:
        job = q.get_nowait()
    except Empty:
        job = None
    return job

def notify_caption_completion(audio_slice_name):
    content = {"request-type" : "caption_completion",
                "source" : SELF_IP,
                "audio-file-name" : audio_slice_name}

    response = requests.post("http://" + master_ip + ":16000", headers = HEADERS, json = content)
    if (response.status_code != 200):
        print (stamp("Master did not acknowledge peer's completion of job {}.".format(audio_slice_name)))
        return False
    return True

# This puts the captions in the dht, run on main thread
async def action_loop():
    while True:
        if caption_queue.qsize() > 0:
            res = try_get_nowait(caption_queue)
            while res:
                audio_file_name, caption = res
                t1 = time.monotonic()
                await node.set(audio_file_name, caption)
                # Tell Master job has been completed
                notify_caption_completion(audio_file_name)
                res = try_get_nowait(caption_queue)
        await asyncio.sleep(0.2)

pygame.mixer.init()

# mainloop here
if mode == "caption":
	try:
		# start request handling thread
		server_thread = Thread(target=run_server)
		server_thread.start()

		# start speech transcription thread
		transcription_thread = Thread(target=process_jobs)
		transcription_thread.start()

		# register self with master
		peer_id = send_join_request()
		send_caption_request(audio_file_name)

		# Join the Kademlia P2P network for the DHT
		loop = asyncio.get_event_loop()
		peer_kademlia_join()

		loop.create_task(action_loop())

		loop.run_forever()

	except KeyboardInterrupt:
		server_thread.stop()
		transcription_thread.stop()
		node.stop()
		loop.close()
		print("Exiting.")
		quit()

	quit()

block_blob_service.get_blob_to_path(container_name, audio_file_name, audio_file_name)
print("Downloaded file {}.".format(audio_file_name))

loop = asyncio.get_event_loop()
server = Server()
loop.run_until_complete(server.listen(5678))
bootstrap_node = (master_ip, port)
loop.run_until_complete(server.bootstrap([bootstrap_node]))

async def get_caption(audio_slice_name):
	res = await server.get(audio_slice_name)
	return res
	
async def get_captions(audio_file_name, num_captions):
	just_name = audio_file_name.split('.')[0]
	captions_keywords = await asyncio.gather(*[get_caption("{}_{}_{}.wav".format(just_name, num_captions, i)) for i in range(num_captions)])
	return captions_keywords

def peer_kademlia_join():
	temp = Server()
	loop.run_until_complete(temp.listen(5678))
	loop.run_until_complete(temp.bootstrap([(master_ip, 5678)]))
	msg = stamp("{} has joined the P2P network.".format(peer_name))
	send_to_hub(msg)
	print(msg)
	global node
	node = temp



# TODO: look up num_captions
new_audio = AudioSegment.from_wav(audio_file_name)
duration = 10 * 1000
num_captions = int (math.ceil(float(len(new_audio)) / duration ))
	
captions_keywords = loop.run_until_complete(get_captions(audio_file_name, num_captions))
captions = [x.split('|')[0] for x in captions_keywords]
summary = str.join(", ", [x.split('|')[1] for x in captions_keywords])
print(captions)
print(summary)

server.stop()
loop.close()


pygame.mixer.music.load(audio_file_name)
pygame.mixer.music.play()

root = Tk()

i = 0
clock = tk.Label(root, bg='white', width=100, height=20, justify=LEFT, wraplength=800, font=(None,15))
clock.pack()
summary_disp_text = "{}\n\n\n\n\n\nSummary: {}".format(captions[i], summary)
clock.config(text=summary_disp_text)
time1 = time.monotonic()
i+=1
def tick():
	global time1
	global i
	# get the current local time from the PC
	time2 = time.monotonic()
	# if time string has changed, update it
	if time2 - time1 >= 10 and i < len(captions):
		time1 = time2
		summary_disp_text = "{}\n\n\n\n\n\nSummary: {}".format(captions[i], summary)
		clock.config(text=summary_disp_text)
		i += 1
	elif i == len(captions):
		root.quit()
		quit()
	# calls itself every 200 milliseconds
	# to update the time display as needed
	# could use >200 ms, but display gets jerky
	clock.after(200, tick)
tick()
root.mainloop()
