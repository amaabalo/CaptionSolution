#!/bin/bash
az account set --subscription "Azure for Students"
az iot edge set-modules --device-id Master --hub-name CaptionHub2 --content ./config/master.deployment.amd64.json
sleep 5
az iot edge set-modules --device-id Peer1 --hub-name CaptionHub2 --content ./config/peer1.deployment.amd64.json
az iot edge set-modules --device-id Peer2 --hub-name CaptionHub2 --content ./config/peer2.deployment.amd64.json
