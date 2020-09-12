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
from get_modbus_tags import read_ext_config
from sub_and_pub import MqttClient
import faulthandler; faulthandler.enable()

logger = logging.getLogger(__name__)


obj_ext_configuration = read_ext_config()


def main():

    mqtt = MqttClient(conf = obj_ext_configuration)
    parse_conf= mqtt.parse_configuration()
   
    mqtt.init_mqtt_client()
    mqtt.connect_mqtt_broker()
    mqtt.subs_tags_callback()
    mqtt.publish_mqtt()
    
if __name__ == '__main__':
    main()


