# moxaeuiiot-connector-se-machine-advisor
Connecting Modbus devices using Moxa UC-2100 low cost IIoT gateway to Schneider EcoStruxure Machine Advisor

Getting started:

1. Download and Install latest release ThingsPro Gateway software v2.6
2. Configure Modbus Data Template and Connection via ThingsPro Gateway web GUI
3. Register your Machine on SE Machine Advisor and get the broker configuration
4. Modify the config.json file and copy to gateway /home/moxa via File transfer e.g. WinSCP
5. Save EcoStruxure file to your laptop and upload it via ThingsPro Gateway web GUI

## 1. Download and Install ThingsPro Gateway v2.6 
```
https://www.moxa.com/en/products/industrial-computing/system-software/thingspro-2#resources
```
## Download TPG
   ![](media/tpg-download-website.png)

## Installation Manual
   ![](media/tpg-installation-manual.png)

## 2. Configure Modbus Device Data Template and Connection to your Modbus Slave

      Follow user manual to configure modbus settings for data acquisistion on page 42
      
## 3. Register your Machine on SE Machine Advisor and get the broker configuration
```
https://www.se.com/ww/en/work/services/field-services/industrial-automation/oem/machine-advisor.jsp
```
