#!/usr/bin/env python
import time
import json
import ssl
import sys
import signal
from Queue import Queue, Empty
import paho.mqtt.client as mqtt
from libmxidaf_py import TagV2
from get_modbus_tags import getModbusTags

tagv2 = TagV2.instance()
msgQueue = Queue(100)
tagList, equipmentNameList = getModbusTags()

def on_tag_callback(equipment_name, tag_name, tag):
    print("Subscribe message = equipment_name: {}, tag_name: {}, at: {}, value: {}, unit: {} ".format(
        equipment_name,
        tag_name,
        tag.at(),
        tag.value(),
        tag.unit()
    ))
   
    # unit() and at() swapped in some version of ThingsPro GW  
     
    str_value = str(tag.value())
    float_tagValue = float(str_value)
  
    payload_format_charlie = {
       "metrics":{
        "assetName": cfg["things-pro"]["asset_name"],
        tag_name:  float_tagValue,
        tag_name +"_timestamp": int(round(time.time() * 1000))
       }
     }
    
    # The subscribe msg will not be queued until client has active connection to the broker. 
    # This will solve the queue full raise error issue when connection to broker has failed for some reason.     
    if client.DISCONNECTED_FLAG != True: 
        msgQueue.put_nowait(json.dumps(payload_format_charlie))


def signal_handler(sig, frame):
    print('Exit')
    sys.exit(0)


# Called when client receives a CONNACK response from server/broker
def on_connect_callback(client, userdata, flags, rc):
    
    if rc == 0:
       
        client.DISCONNECTED_FLAG = False
        print ("DISCONNECTED_FLAG : {}".format(client.DISCONNECTED_FLAG)) 
        print ("Connected successfully to broker: {}".format(cfg["general"]["broker"]))
        
        # Equipment tags
        print "tag_List :", tagList          
        print "equipmentName_List :", equipmentNameList
        
    # It is make sense to stop the background thread network loop 
    # to avoid getting the same error when connection refused
    elif rc == 1:
        print("Connection refused - incorrect protocol version")
        client.loop_stop()
        
    elif rc == 2:
        print("Connection refused - invalid client identifier")
        client.loop_stop()
        
    elif rc == 3:
        print("Connection refused - server unavailable")
        client.loop_stop()
        
    elif rc == 4:
         print("Connection refused - bad username or password")
         client.loop_stop()
         
    else:
         print("Connection refused - not authorised") 
         client.loop_stop()   
  

def on_disconnect_callback(client, userdata, rc):
    client.DISCONNECTED_FLAG = True
    print("Disonnected with result code = " +str(rc))
   

def on_log(client, userdata, level, buf):
    print("log: ",buf)



"""
Configuration 
"""
with open("config.json") as json_data_file:
    cfg = json.load(json_data_file)


"""
Calling Tag callback functions
"""
tagv2.subscribe_callback(on_tag_callback)


"""
Subscribe System tags
"""
# subscribe((str)equipment_name, (str)tag_name)

if (cfg["things-pro"]["system_status"]) == True :
    tagv2.subscribe("SYSTEM", "cpu_usage")
    tagv2.subscribe("SYSTEM", "memory_usage")
    tagv2.subscribe("SYSTEM", "disk_usage")


"""
Subscribe Modbus tags
"""
print "tag_List :", tagList          
print "equipmentName_List :", equipmentNameList

# tagv2.subscribe("ModSlave-Device", "temp")
for tag_name in tagList:
    tagv2.subscribe(cfg["things-pro"]["device_name"], tag_name)  


"""
Initialize Connection 
"""
client = mqtt.Client(client_id= cfg["general"]["client_id"], clean_session= cfg["general"]["clean_session"], userdata=None)


"""
Create deafult Flags
"""
client.DISCONNECTED_FLAG = False


"""
Credentials
"""
client.username_pw_set(username= cfg["credentials"]["user_name"], 
                       password= cfg["credentials"]["password"] )


"""
Calling callback functions
"""
client.on_connect = on_connect_callback
client.on_disconnect = on_disconnect_callback
#client.on_log = on_log


"""
Configure SSL/TLS
"""
if (cfg["ssl/tls"]["enable_tls"]) == True :
       #client.tls_set_context(context=None)
       client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=None,  tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
       client.tls_insecure_set(cfg["ssl/tls"]["tls_insecure_set"])


"""
Connect to broker
"""
ret=client.connect(host= cfg["general"]["broker"], 
                    port= cfg["general"]["port"], 
                    keepalive= cfg["general"]["keep_alive_sec"])

print ("Connecting to broker: {}".format(cfg["general"]["broker"]))


"""
Starting network loop
"""
client.loop_start()

      
"""
Calling signal handler
"""
signal.signal(signal.SIGINT, signal_handler)


while(True):
    
    if client.DISCONNECTED_FLAG != True:
        try:
            msg = msgQueue.get(block=False)
             
            """ 
            Publish message
            """         
            client.publish("devices/" + cfg["general"]["client_id"] + "/messages/events/", msg)
            print("Publish message format charlie = {}".format(msg))
            
        except Empty as e:
            time.sleep(1)
            continue
        