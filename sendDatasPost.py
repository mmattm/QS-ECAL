# -*- coding: utf-8 -*-
import time 
import socket
from bson import json_util
import sqlite3
import json
import time
import threading
import datetime
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import requests
from subprocess import check_output
import os
import logging

def totimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6 * 1000

def current_time_stamp():
    now = datetime.utcnow()
    return int(totimestamp(now))

# -- Global vars --

count = 0
recall_time = 0.7
json_file_name='/home/pi/Desktop/lastTimeStamp.json'
database = '/home/pi/easyiot/EasyIoTDatabase.sqlite'
request = """SELECT raw_values.id AS id,
     domains.domain AS domain,
     addresses.address AS address,
     properties.property AS property,
     raw_values.timestamp as timestamp,
     raw_values.value as value
  FROM
  raw_values, properties, domains, addresses
  WHERE
  domains.domainid = raw_values.domain AND
  addresses.addressid = raw_values.address AND
  properties.propertyid = raw_values.property AND timestamp > ?
  ORDER BY raw_values.id ASC;"""

# --- Code ---

# Globals

SERVER_ADDR = "178.62.253.111"
LOG_FILENAME="/home/pi/Desktop/quantifiedSchool.log"

def send_server_infos():
  logging.debug("Checking for new values at " + str(time.time()) + " ...")
  
  timestamp = current_time_stamp()

  try:
    json_data = open(json_file_name)
    data = json.load(json_data, object_hook=json_util.object_hook)
    timestamp = data["timestamp"]
    logging.debug("the timestamp from json is " + str(timestamp))
    json_data.close()

#

#    if timestamp == 0:
#        timestamp=current_time_stamp()

    
  except IOError as e:   
    logging.error('there is an error with reading the json file')
    os.system('touch '+json_file_name)
  except ValueError as e:
    logging.error('Value error')
  conn = sqlite3.connect(database, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
  c = conn.cursor()

  #TODO refactor into a function and call every second
  for row in c.execute(request, (timestamp,)):
    logging.debug("for loop")
    id = row[0]
    domain = row[1]
    address = row[2]
    property = row[3]
    ts = row[4]
    value = row[5]
    
    logging.debug(row)
    end_timestamp = 5 * 3600000 + ts # the static value is 5 hours of offset for easyiot bug, it's in milis so
    logging.debug("Sending datas with id:" + str(id) + " at timestamp " +str(end_timestamp) + " : ")
    #logging.debug(str(end_timestamp))
    data_entry = { "domain": domain, "address": address, "property":property, "timestamp":end_timestamp, "value":value }
    logging.debug(data_entry)
    #data is the name of the collection
    requests.post("http://" + SERVER_ADDR + "/add", data=data_entry)
    logging.debug('')
##    timestamp = current_time_stamp()
    timestamp = ts
    with open(json_file_name, 'w') as outfile:
      json.dump({"timestamp":timestamp}, outfile, default=json_util.default)

  threading.Timer(recall_time, send_server_infos).start()

# --- Main ---

def send_ip():
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect(('google.com', 0))
  ip = s.getsockname()[0]
  myMAC = open('/sys/class/net/eth0/address').read()
  pi_infos= {'mac':myMAC.rstrip(), 'ip':ip }
  requests.post("http://" + SERVER_ADDR + "/updatePi", data=pi_infos)
  logging.debug('IP ' + str(ip) + ' sent to server.') 
  return ip   

def main():
  global count
  print("Starting quantified school, check " + LOG_FILENAME + " for execution details. ")
  #Logging setup
  
  #Creates a file if does not exist
  os.system("touch " + LOG_FILENAME) 
  logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
  
  try:
    send_ip()
    count = 0
  except Exception as e: 
    logging.debug("Failed to send ip address") 
    logging.debug(e)

    restart()
    time.sleep(3) 
    main()

  try:
    send_server_infos()
    count = 0
  except Exception as e:
    logging.debug(e)
    restart()
    main()


def restart():
  global count
  count = count + 1
  if count > 20:
    os.system("sudo reboot")

if __name__ == "__main__":
  main()
