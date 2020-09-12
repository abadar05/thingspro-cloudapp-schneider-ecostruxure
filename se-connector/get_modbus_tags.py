#!/usr/bin/env python 3
"""
Get Modbus Tag List and Equipment Name List"
"""

__author__ = "Amjad B."
__license__ = "MIT"
__version__ = '1.3'
__status__ = "beta"

import requests
import json
import os
import logging


logger = logging.getLogger(__name__)

tagList = []
equipmentNameList = []


def get_mx_api_token():
    try:
        file = open('/etc/mx-api-token', 'r')
        token = file.readline()
        logger.info("Read MX-API-Token: {}".format(token))
        file.close()
        return token
    except IOError as e:
        logger.error(e)
        return None


def read_ext_config():
    """
    Configuration
    """
    try:
        with open("/home/moxa/config.json") as json_data_file:
            cfg_obj = json.load(json_data_file)
            # logger.debug("Read External Config: {}".format(cfg_obj)) 
            return cfg_obj
    except IOError as e:
        logger.error(e)
        

def get_modbus_tags():
    
   
    cfg = read_ext_config() 
    
    """
    Get local API Token
    """
    token = get_mx_api_token()
    
    """
    Header
    """    
    headers = {"mx-api-token": token}  
    
    """
    GET 
    """    
    r = requests.get('https://localhost/api/v1/mxc/equipments', headers=headers, verify=False)
    data = r.json()
    
    if r.status_code == 200:
        data = r.json()
        logger.info('Query Equipments successfull')
        
        """
        Parser
        """
        for dictionary in data:
            get_keys_list = (dictionary.keys())
       
            get_equipmentName =  dictionary['equipmentName']  
       
            equipmentNameList.append(get_equipmentName) 
        
            if dictionary['equipmentName'] == cfg["things-pro"]["device_name"]:
                get_equipmentTags = dictionary['equipmentTags']
  
                for nested_tag in get_equipmentTags:
                    get_nested_equipmentTags = nested_tag
                
                    get_tagName = get_nested_equipmentTags['name']
                 
                    tagList.append(get_tagName)
                    
        logger.info("Tag List : {}".format(tagList)) 
        logger.info("Equipment Name List : {}".format(equipmentNameList)) 
        return tagList, equipmentNameList
            
    else:
        logger.error('Host URL with error! http status code %s', r.status_code)
       
    

