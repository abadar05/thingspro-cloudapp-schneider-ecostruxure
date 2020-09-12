#!/usr/bin/env python 3
"""
Publish Modbus Data to Schneider EcoStruxure Cloud"
"""

__author__ = "Amjad B."
__license__ = "MIT"
__version__ = '1.3'
__status__ = "beta"

import time
import json
import ssl
import sys
import signal
import logging
from libmxidaf_py import TagV2
import paho.mqtt.client as mqtt_client
from get_modbus_tags import get_modbus_tags
from collections import deque
   

format = '%(asctime)s: %(levelname)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, datefmt="[%Y-%m-%d %H:%M:%S]", format=format)
logger = logging.getLogger(__name__)


# ToDo
# Send Southbound Modbus Device Connection Status


class MqttBaseCLass():
 
    def __init__(self, **kwargs):
        
        self._ext_conf = kwargs.get('conf', None)
        logger.debug("Read External Config: {}".format(self._ext_conf))        
        
        self._client = None
        self._userdata = None
        self._mqqtconnect = None
        self.IS_CONNECTED = False
        
        self._broker_url = None
        self._client_id = None
        self._clean_session = False
        self._keep_alive_sec = 60
        self._publish_interval = 5
        self._broker_username = None
        self._broker_password = None
        self._broker_enable_tls = True
        self._broker_insecure_tls = False
        self._asset_name = None
        self._device_name = None
        self._system_status = None
        
    def parse_configuration(self):
        """
        Parse config.json file 
        """
       
        if not self._ext_conf:
            self.log_error('Empty configuration!')
            return False          
        
        # continue here 
        if self._ext_conf["general"]["broker"]: 
            self._broker_url = self._ext_conf["general"]["broker"] 
            logger.debug("broker: {}".format(self._broker_url))
            
        if self._ext_conf["general"]['client_id']:
            self._client_id = self._ext_conf["general"]['client_id']
            logger.debug("client_id: {}".format(self._client_id))
        
        if self._ext_conf["general"]['clean_session']:
            self._clean_session = self._ext_conf["general"]['clean_session']
            logger.debug("clean_session: {}".format(self._clean_session))
            
        if self._ext_conf["general"]['keep_alive_sec']:
            self._broker_keepalive = self._ext_conf["general"]['keep_alive_sec'] 
            logger.debug("keep_alive_sec: {}".format(self._broker_keepalive))
            
        if self._ext_conf["general"]['publish_interval']:
            self._publish_interval = self._ext_conf["general"]['publish_interval'] 
            logger.debug("publish_interval: {}".format(self._publish_interval))
                        
        if self._ext_conf["credentials"]['user_name'] :
            self._broker_username = self._ext_conf["credentials"]['user_name'] 
            logger.debug("user_name: {}".format(self._broker_username))
                
        if self._ext_conf["credentials"]['password']:
            self._broker_password = self._ext_conf["credentials"]['password']
            logger.debug("password: {}".format(self._broker_password))
                
        if self._ext_conf["ssl/tls"]['enable_tls']:
            self._broker_enable_tls = self._ext_conf["ssl/tls"]['enable_tls']
            logger.debug("enable_tls: {}".format(self._broker_enable_tls))
                
        if self._ext_conf["ssl/tls"]['tls_insecure_set']:
            self._broker_insecure_tls = self._ext_conf["ssl/tls"]['tls_insecure_set']
            logger.debug("tls_insecure_set: {}".format(self._broker_insecure_tls))
                
        if self._ext_conf["things-pro"]['asset_name']:
            self._asset_name = self._ext_conf["things-pro"]['asset_name']
            logger.debug("asset_name: {}".format(self._asset_name))
                 
        if self._ext_conf["things-pro"]['device_name']:
            self._device_name = self._ext_conf["things-pro"]['device_name']
            logger.debug("device_name: {}".format(self._device_name))
                
        if self._ext_conf["things-pro"]['system_status']:
            self._system_status = self._ext_conf["things-pro"]['system_status']
            logger.debug("system_status: {}".format(self._system_status))
            
        logger.info("Parse Configuration Successfull! ")
        return True 


    def init_mqtt_client(self):

        """
        Initilaize mqtt client configuration
        """
        logger.info("Create MQTT Client {}, clean session = {}" .format(self._client_id, self._clean_session))
        self._client = mqtt_client.Client(client_id= self._client_id, clean_session= self._clean_session, userdata=self._userdata)
        
        logger.info('Register Callback functions')
        
        self._client.on_connect = self.on_connect_callback
        self._client.on_disconnect = self.on_disconnect_callback
        self._client.on_publish = self.on_publish_callback
        #self._client.on_log = self.on_log
      
        if self._broker_username:
            logger.info('Set User Name and Password')
            self._client.username_pw_set(username= self._broker_username, password= self._broker_password )

        if self._broker_enable_tls:
            logger.info('Enable TLS, insecure_tls=%s', self._broker_insecure_tls)
            self._client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=None,  tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
            self._client.tls_insecure_set(self._broker_insecure_tls)
        
        self._client.loop_start()
        
        return True
        
        
    def connect_mqtt_broker(self):
        """
        Initilaize request for connecting to the Schneider Machine Advisor Broker
        """
   
        # Parse schneider broker url
        broker_host = self._broker_url.split("//")[1]

        url = broker_host.split(":")[0] 
        port = broker_host.split(":")[1] 

        # Convert from unicode into integer
        port = int(str(port))
        self._mqqtconnect = self._client.connect(host= url, port= port, keepalive= self._keep_alive_sec)
        logger.info("Connecting to broker: {}".format(self._broker_url))
        
        
    def on_connect_callback(self, client, userdata, flags, rc):
        """
        This function is called when client is connected to Schneider Machine Advisor Broker
        """     
        logger.info('OnConnect! rc={}, flags={}'.format(rc, flags))     
        if rc == 0:           
            self.IS_CONNECTED = True
            logger.info("Connected successfully to broker: {}".format(self._broker_url))
           
            
        elif rc == 1:
            logger.error("Connection refused - incorrect protocol version, result code: {}".format(rc)) 
            self._client.loop_stop()
               
        elif rc == 2:
            logger.error("Connection refused - invalid client identifier, result code: {}".format(rc)) 
            self._client.loop_stop() 
             
        elif rc == 3:
            logger.error("Connection refused - server unavailable, result code: {}".format(rc)) 
            self._client.loop_stop()
            
        elif rc == 4:
            logger.error("Connection refused - bad username or password, result code: {}".format(rc))
            self._client.loop_stop()
            
        else:
            logger.error("Connection refused - not authorised, result code: {}".format(rc)) 
            self._client.loop_stop()
            
                      
    def on_disconnect_callback(self, client, userdata, rc):
        """
        This function is called when client is disconnected from Schneider Machine Advisor Broker
        """
        
        logger.info('OnDisonnect! rc = %s', rc)
        self.IS_CONNECTED = False
        if rc:
            logger.error('Disonnected with result code = {}'.format(rc))  
        return

    def on_log(self, client, userdata, level, buf):
        logger.info("OnLog(%s): %s ", level, buf)
        return
        
    def on_publish_callback(self, client, userdata, mid):
        """
        This function is called when message is publish to Schneider Machine Advisor Broker
        """
        logger.debug('OnPublish MsgID [%s]', mid)
    
    def is_open(self):
        """
        To check existing client connection to Schneider Machine Advisor Broker
        """
        if self._client and self.IS_CONNECTED == True:
            return True
        else:
            return False

   
class SubscribeTags(MqttBaseCLass):

    def __init__(self, **kwargs):
        """
        Access Init() propties of Mqtt base class
        Example: self._asset_name,  self._system_status
        
        """
        
        MqttBaseCLass.__init__(self, **kwargs)
    
        self._system_tags = None  
        
        self._tagv2 = TagV2.instance()
     
        
        self._vtag_tags = 0 
        self._vtag_data = deque(maxlen=kwargs.get('max_data_size', 100))
        
        
               
    def build_mqtt_msg_charlie(self):   
        """
        Send one aggregated message every N publish interval    
        Convert tag into float as Machine Advisor either support int or float data type
        """
        payload = {
            "metrics": {
                "assetName": self._asset_name
                }
            }      
        while len(self._vtag_data)>0:
            tag = self._vtag_data.popleft()   
            # logger.debug("tag :", tag)
            payload['metrics'][tag['name']] = float(str(tag['value']))
            #print("Type tag['value'] :", str(tag['value']))
            payload['metrics'][tag['name'] + "_timestamp"] = tag['timestamp']
        
        logger.debug("build_mqtt_msg_charlie :", payload)
        return json.dumps(payload)
        
    
    def tpg_callback(self, source_name, tag_name, Tag):
        """
        A callback function called on every subscribe message from ThingsPro
        """
        
        logger.debug("Subscribe = equipment_name: {}, tag_name: {}, at: {}, value: {}".format(
            source_name,
            tag_name,
            Tag.at(),
            Tag.value()
        ))
        
        self._vtag_tags += 1
        timestamp=int(time.time()*1000)
        self._vtag_data.append({'name': tag_name, 'value': Tag.value(), 'timestamp': timestamp})
        return

         
    def subs_tags_callback(self):
        """
        Calling the callback function
        """
        self._tagv2.subscribe_callback(self.tpg_callback) 
        
   

class MqttClient(SubscribeTags):

    def __init__(self, **kwargs):
        SubscribeTags.__init__(self, **kwargs)
               
        self._mbus_tagList, equipmentNameList = get_modbus_tags()


    def publish_mqtt(self):
        """
        This is main function that publish mqtt messages in charlie format to Schneider Machine Advisor Broker
        """
        logger.info('Run publish_mqtt! ...')         
        
        logger.debug("SYSTEM_STATUS: {}".format(self._system_status))
            
        if self._system_status:
            self._system_tags =["cpu_usage", "memory_usage", "disk_usage"]
            for tag_name in self._system_tags:
                logger.info('TPG Subscribe for SYSTEM - %s', tag_name)            
                self._tagv2.subscribe("SYSTEM", tag_name)
        
        for tag_name in self._mbus_tagList:
            logger.info("TPG Subscribe for {} - {}".format(self._device_name, tag_name))  
            self._tagv2.subscribe(self._device_name, tag_name)  

                 
        logger.info('Publish TPG tags every %s seconds', self._publish_interval)
        self._run = True
        while self._run == True:
            time.sleep(self._publish_interval)
            
            logger.info('%d Tags in Queue, total recived %d', len(self._vtag_data), self._vtag_tags)
            msg = self.build_mqtt_msg_charlie()
            
            mqtt_connection = self.is_open() 
            if  mqtt_connection == True:
                logger.info("Publish = {}".format(msg))
                logger.info("Mqtt Connection Open! {}".format(mqtt_connection)) 
                logger.info("Publish Interval = {}".format(self._publish_interval)) 
                self._client.publish("devices/" + self._client_id + "/messages/events/", msg)
            else:    
                logger.error("Mqtt Connection Closed! {}".format(mqtt_connection))
            
        return True
    
      
    
    
           
        

 
             
