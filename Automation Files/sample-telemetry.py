import argparse
import logging
import json
from os import spawnl
import uuid
import sys
import csv
import sys
import datetime
import time as t
from collections import namedtuple
from pathlib import Path
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    QOS,
    PublishToIoTCoreRequest,
    IoTCoreMessage,
    SubscribeToIoTCoreRequest
)


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

TIMEOUT = 60

ipc_client = awsiot.greengrasscoreipc.connect()

logger.info("Generate Telemetry is ONLINE")
  
class StreamHandler(client.SubscribeToIoTCoreStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        logger.info("Generate Telemetry received an event")

        # Handle message.
        self.start_telemetry(event)
        

    def on_stream_error(self, error: Exception) -> bool:
        logger.info(str(error))
        # Handle error.
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        pass

    def start_telemetry(self, event: IoTCoreMessage) -> None:
        logger.info("Start Telemetry")
        maxspeed = 0
        totalspeed = 0
        avg_speed = 0
        counter = 0
        message = str(event.message.payload, "utf-8")
        
        payload = json.loads(message)
        vin = payload['vin']
        CLIENT_ID = vin

        trip_topic = "dt/cvra/{}/trip".format(vin)
        topic = "dt/cvra/{}/cardata".format(vin)
        qos = QOS.AT_LEAST_ONCE
        logger.info("Create publish Request for VIN: {}".format(vin))
        request = PublishToIoTCoreRequest()
        request.topic_name = topic
        request.qos = qos
        trip_request = PublishToIoTCoreRequest()
        trip_request.topic_name = trip_topic
        trip_request.qos = qos
        logger.info("Publish Request created..")
        logger.info("Connecting to AWS IoT ...")
        ipc_client = awsiot.greengrasscoreipc.connect()
        logger.info("Connected to AWS IoT ...")

        tripId = uuid.uuid4().hex
        logger.info("Generating Trip ID of {}".format(tripId))

        #setup trip payload
        p = Path(__file__).with_name('trip_payload.json')
        with p.open('r') as f:
            trip_template = json.load(f)
        
        trip_template["vin"] = vin
        trip_template["tripid"] = tripId
        trip_template["sendtimestamp"] = str(datetime.datetime.now().astimezone().isoformat())

        telemetry = namedtuple("telemetry", ['time', 'torque','motor_torque','accel','decel','speed_mph','batt_soc','batt_current','x_pos','y_pos','odometer', 'remaining'])
        a_list = []
        p = Path(__file__).with_name('vehicle_data_variables_50ms_sampling.csv')
        with p.open('r') as csvfile:
            reader = csv.reader(csvfile,delimiter=",")
            for row in reader:
                c = telemetry(float(row[0]),float(row[1]),float(row[2]),float(row[3]),float(row[4]),float(row[5]),float(row[6]),float(row[7]),float(row[8]),float(row[9]), float(row[10]), float(row[11]))
                a_list.append(c)
                        
        latLongDict = a_list

        for i in latLongDict:
            p = Path(__file__).with_name('payload.json')
            with p.open('r') as f:
                template = json.load(f)
            logger.info("Setting up payload")

            if i.speed_mph > maxspeed:
                maxspeed = i.speed_mph
            
            logger.info("Calculate Avg Speed")
            totalspeed = totalspeed + i.speed_mph
            avg_speed = (totalspeed / (counter + 1))
            ts = str(datetime.datetime.now().astimezone().isoformat())
            if counter == 0:
                logger.info("Setup start of trip payload")
                trip_template["tripsummary"]["startlocation"]["latitude"] = i.x_pos/100
                trip_template["tripsummary"]["startlocation"]["longitude"] = i.y_pos/100
                trip_template["tripsummary"]["starttime"] = ts
                trip_template["creationtimestamp"] = ts
            
            logger.info("Complete payload setup")
            template["MessageId"] = vin + '-' + ts
            template["SimulationId"] = tripId
            template["TripId"] = tripId
            template["CreationTimeStamp"] = ts
            template["SendTimeStamp"] = ts
            template["GeoLocation"]["Latitude"] = i.x_pos/1000
            template["GeoLocation"]["Longitude"] = i.y_pos/1000
            template["GeoLocation"]["Speed"] = i.speed_mph
            template["Speed"]["Average"] = avg_speed
            template["Speed"]["Max"] = maxspeed
            template["VIN"] = vin
            template["Odometer"]["Metres"] = i.odometer
            template["ChargeRemaining"] = i.remaining
            template["batterySOC"] = i.batt_soc
            
            logger.info("Payload setup: {}".format(template))
            payload = json.dumps(template)
            logger.info("Payload setup: {}".format(payload))
            request.payload = bytes(payload, "utf-8")
            logger.info("Payload set to request.")
            operation = ipc_client.new_publish_to_iot_core()
            operation.activate(request)
            future = operation.get_response()
            future.result(TIMEOUT)
            logger.info("Successfully published coordinates {} of {}".format(i, len(latLongDict)))
            t.sleep(1)
            counter=counter+1
        
        trip_template["tripsummary"]["endlocation"]["latitude"] = i.x_pos/100
        trip_template["tripsummary"]["endlocation"]["longitude"]= i.y_pos/100
        trip_payload = json.dumps(trip_template)

        trip_request.payload = bytes(trip_payload, "utf-8")
        operation = ipc_client.new_publish_to_iot_core()
        operation.activate(trip_request)
        future = operation.get_response()
        future.result(TIMEOUT)
        logger.info("Successfully published trip")

logger.info("Defining command topic")
 
topic = "dt/cvra/ggv2demo/command"
qos = QOS.AT_MOST_ONCE

request = SubscribeToIoTCoreRequest()
request.topic_name = topic
request.qos = qos

logger.info("Subscribe request created")
 
handler = StreamHandler()
operation = ipc_client.new_subscribe_to_iot_core(handler)
future = operation.activate(request)
future.result(TIMEOUT)

logger.info("IPC client handler connected.")

# Keep the main thread alive, or the process will exit.
while True:
    t.sleep(10)

logger.info("Got here ")

# To stop subscribing, close the operation stream.
operation.close()