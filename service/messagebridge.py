### service/messagebridge: interact with Ciseco/WirelessThings devices
## HOW IT WORKS: 
## DEPENDENCIES:
# OS: 
# Python: 
## CONFIGURATION:
# required: port_listen, port_send
# optional: 
## COMMUNICATION:
# INBOUND: 
# - IN: 
#   required: node_id, measure
#   optional: cycle_sleep_min
# - OUT: 
#   required: node_id, measure
#   optional: cycle_sleep_min
# OUTBOUND: 
# - controller/hub IN: 
#   required: node_id, measure
#   optional: cycle_sleep_min

import datetime
import json
import sys
import time
import os
import socket
import json

from sdk.python.module.service import Service
from sdk.python.utils.datetimeutils import DateTimeUtils
from sdk.python.module.helpers.message import Message

import sdk.python.utils.exceptions as exception

class Messagebridge(Service):
    # What to do when initializing
    def on_init(self):
        # configuration
        self.config = {}
        # queue messages when the sensor is sleeping
        self.queue = {}
        # require configuration before starting up
        self.config_schema = 1
        self.add_configuration_listener(self.fullname, "+", True)
        
    # transmit a message to a sensor
    def tx(self, node_id, data, service_message=False):
        if not module_message: log_info("sending message to "+node_id+": "+str(data))
        # create a socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # prepare the message
        message = {'type':"WirelessMessage",'network':"Serial"}
        message["id"] = node_id
        message["data"] = data if isinstance(data,list) else [data]
        json_message = json.dumps(message)
        # send the message
        self.log_debug("sending message: "+json_message)
        sock.sendto(json_message, ('<broadcast>', self.config["port_send"]))
        sock.close()
        
    # What to do when running
    def on_start(self):
        self.log_debug("listening for UDP datagrams on port "+str(self.config["port_listen"]))
        # bind to the network
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", self.config["port_listen"]))
        while True:
            try:
                # new data arrives	
                data, addr = sock.recvfrom(1024)
                self.log_debug("received "+data)
                # the message is expected to be in JSON format
                data = json.loads(data)
                if data["type"] != "WirelessMessage": continue
                # parse content
                content = data["data"][0]
                # sensor just started
                if content == "STARTED":
                    self.log_info(data["id"]+" has just started")
                    self.tx(data["id"], "ACK", True)
                elif content == "AWAKE":
                    # send a message if there is something in the queue
                    if data["id"] in self.queue and len(self.queue[data["id"]]) > 0:
                        self.tx(data["id"], queue[data["id"]])
                        self.queue[data["id"]] = []
                else:
                    # look for the sensor_id associated to this measure
                    sensor = None
                    for sensor_id in self.sensors:
                        if data["id"] == self.sensors[sensor_id]["node_id"] and content.startswith(self.sensors[sensor_id]["measure"]):
                            sensor = self.sensors[sensor_id]
                            break
                    # if not registered, skip it
                    if sensor is None: continue
                    # prepare the message
                    message = Message(self)
                    message.recipient = "controller/hub"
                    message.command = "IN"
                    message.args = sensor_id
                    # generate the timestamp
                    # date_in = datetime.datetime.strptime(data["timestamp"],"%d %b %Y %H:%M:%S +0000")
                    # message.set("timestamp", int(time.mktime(date_in.timetuple())))
                    # strip out the measure from the value
                    message.set("value", content.replace(self.sensors[sensor_id]["measure"],""))
                    # send the measure to the controller
                    self.send(message)
            except Exception,e:
                self.log_warning("unable to parse "+str(data)+": "+exception.get(e))
            
    # What to do when shutting down
    def on_stop(self):
        pass
        
    # What to do when receiving a request for this module
    def on_message(self, message):
        sensor_id = message.args
        if message.command == "OUT":
            sensor = message.get_data()
            data = message.get("value")
            if not self.is_valid_configuration(["node_id", "measure"], sensor): return
            if "cycle_sleep_min" not in sensor:
                # send the message directly
                self.tx(sensor["node_id"], data)
            else:
                # may be sleeping, queue it
                self.log_info("queuing message for "+sensor["node_id"]+": "+data)
                if node_id not in queue: queue[node_id] = []
                queue[node_id] = [data]

    # What to do when receiving a new/updated configuration for this module
    def on_configuration(self,message):
        # module's configuration
        if message.args == self.fullname and not message.is_null:
            if message.config_schema != self.config_schema: 
                return False
            if not self.is_valid_configuration(["port_listen", "port_send"], message.get_data()): return False
            self.config = message.get_data()
        # register/unregister the sensor
        if message.args.startswith("sensors/"):
            if message.is_null: 
                sensor_id = self.unregister_sensor(message)
            else: 
                sensor_id = self.register_sensor(message, ["node_id", "measure"])
