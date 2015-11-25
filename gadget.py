# -*- coding:utf-8 -*-

import os

from os import path

import garuda.garudaclientbackend as Garuda
from garuda.garudaclientbackend import GarudaClientBackend
from itertools import count

import glob
import http.client

class GarudaCommunicationHandler():

    backend = None
    gadgetMap = {}
    loopBreak = True

    def __init__(self, gadget_name, gadget_id, *args, **kwargs):
        self.gadget_name = gadget_name
        self.gadget_id = gadget_id
        self.init_backend()

    def init_backend(self):
        try:
            self.backend = GarudaClientBackend(self.gadget_name, self.gadget_id)
            self.backend.add_lisenter(self.garuda_message_handler)
            self.backend.initialize()
        except Exception:
            pass

    def garuda_message_handler(self, message_id, response_code, param):

        if message_id == Garuda.ID_ACTIVATE_GADGET_RESPONSE:
            if response_code == str(Garuda.RESPCODE_SUCCESS):
                print("Gadget Activated ...")
                pass
            else:
                print("Activate Gadget Error: ", response_code)

        elif message_id == Garuda.ID_LOAD_GADGET_REQUEST:
            if not isinstance(param, dict):
                return
            print("Received 'Load Gadget' request ...")
            gadget = param.get("gadget", None)
            path = param.get("path", None)
            if gadget and path:
                self.backend.response_load_gadget(gadget.gadget_name, gadget.gadget_id, Garuda.RESPCODE_SUCCESS)
                
        elif message_id == Garuda.ID_GET_COMPATIBLE_GADGET_LIST_RESPONSE:
            if response_code == str(Garuda.RESPCODE_SUCCESS):
                gadgets = self.backend.get_compatible_gadget_list()
                if gadgets:
                    count = 0
                    for gadget in gadgets:
                        count += 1
                        self.gadgetMap[str(count)] = gadget
                        print("Select " + str(count) + " for sending Data to "+ gadget.gadget_name )
                    gadgetIndex = input("Enter your choice: ")
                    selectedGadget = self.gadgetMap[gadgetIndex]
                    if selectedGadget:
                        data = [os.path.abspath(x) for x in glob.glob("*.xml")]
                        self.backend.send_data_to_gadget(data, selectedGadget.gadget_name, selectedGadget.gadget_id, False)
                    else:
                        print("Invalid Input!!!")
                    
            elif response_code == str(Garuda.RESPCODE_UNABLE_TO_PARSE_JSON):
                print("Error in parsing Gadget information!!!")
            else:
                pass

        elif message_id == Garuda.ID_SEND_DATA_GADGET_RESPONSE:
            if response_code == str(Garuda.RESPCODE_SUCCESS):
                print("Received response for 'Send Data' request ...")
            else:
                print("Send data gadget Error: ", response_code)
            self.loopBreak = False

        elif message_id == Garuda.ID_LOAD_DATA_STREAM_REQUEST:  # is stream
            if not isinstance(param, dict):
                return
            print("Stream Data Received ...")
            
        elif message_id == Garuda.ID_LOAD_DATA_REQUEST:  # no stream
            if not isinstance(param, dict):
                return
            print("Data Received ...")

        elif message_id == Garuda.ID_SEND_NOTIFICATION_TO_GADGET_REQUEST:
            print("Received Notification ...")

        elif message_id == Garuda.ID_CONNECTION_TERMINATED:
            print("Socket Connection Terminated!")

        elif message_id == Garuda.ID_CONNECTION_NOT_INITIALIZED:
            print("Connection to Garuda is starting to initialize")

        elif message_id == Garuda.ID_JSON_PARSE_ERROR:
            print("Json parse error!")

        elif message_id == Garuda.ID_JSON_DUMPS_ERROR:
            print("Json dumps error!")

    def get_gadget_list(self, file_extension, file_type):
        self.backend.request_compatible_gadget_list(file_extension, file_type)

    def terminate(self):
        try:
            self.backend.stop_backend()
        except Exception:
            pass

def download_kgml(app, orgid):
    if len(orgid) > 3:
        print("Your organism code is more than 3alphabet")
    elif orgid == '1':
        app.get_gadget_list("xml", "kgml")
    elif orgid == '0':
        app.terminate()
    else:
        conn = http.client.HTTPConnection('rest.kegg.jp')
        conn.request("GET", "/list/pathway/" + orgid)
        re = conn.getresponse()
        #print(re.status, re.reason)
        if re.status == 200:
            for i in re.readlines():
                pathid = i.decode('utf-8').split("\t")[0]
                pathid = pathid.split(":")[1]
                print("Downloading " + pathid)
                newconn = http.client.HTTPConnection('rest.kegg.jp')
                newconn.request("GET", "/get/" + pathid + "/kgml")
                re = newconn.getresponse()
                handle = open(pathid+".xml", "w")
                handle.write(re.read().decode('utf-8'))
                handle.close()
            print("finishded downloading for organism " + orgid)
        else:
            print("Your organism code is not in KEGG")

print("Starting KEGG client Gadget ...")
app = GarudaCommunicationHandler("KeggClientGadget", "4d62b271-d81d-43fb-849f-65063f2e449c") # For 54 server

userInput = input("Please input 3alphabet KEGG organism code to download KGML, or 1 to send KGMLs, or 0 to Exit:\n")
if userInput != '0':

    download_kgml(app, userInput)
    moreInput = input("Would you like to download KGML for the other organism? If so, please input 3alphabet KEGG organism code once again, or 1 to send KGMLs, or 0 to Exit:\n")

    if moreInput != '0':
        download_kgml(app, moreInput)
    elif len(moreInput) > 3:
        print("Invalid Input!!!")
    elif moreInput == '0':
        app.terminate()

    while app.loopBreak:
        continue

elif len(userInput) > 3:
    print("Invalid Input!!!")
elif userInput == '0':
    app.terminate()
