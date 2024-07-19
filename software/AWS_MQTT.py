
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json
from datetime import datetime
from os import system


#from utils.command_line_utils import CommandLineUtils

# This sample uses the Message Broker for AWS IoT to send and receive messages
# through an MQTT connection. On startup, the device connects to the server,
# subscribes to a topic, and begins publishing messages to that topic.
# The device should receive those same messages back from the message broker,
# since it is subscribed to that same topic.

# cmdData is the arguments/input from the command line placed into a single struct for
# use in this sample. This handles all of the command line parsing, validating, etc.
# See the Utils/CommandLineUtils for more information.
#cmdData = CommandLineUtils.parse_sample_input_pubsub()

received_count = 0
received_all_event = threading.Event()
sensor_dic = {	"01":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"02":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"05":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"06":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"07":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"09":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"10":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5},
				"12":{"time":"00:00:00 2024-05-16","t1":25.5,"t2":25.5,"h1":60.5,"h2":60.5,"co2":400.5}}

humidificador_dic = { 	"03":{"time":"00:00:00 2024-05-16","e":0},
						"08":{"time":"00:00:00 2024-05-16","e":0} }
extractor_dic = {	"02":{"time":"00:00:00 2024-05-16","e1":0,"e2":0,"e3":0},
					"07":{"time":"00:00:00 2024-05-16","e1":0,"e2":0}}

puertas_dic = { "04":{"time":"00:00:00 2024-05-16","p1":0,"p2":0},
				"01":{"time":"00:00:00 2024-05-16","p1":0,"p2":0}}

sensors_room_1 = ['01','02','05','06','12']
sensors_room_2 = ['07','09','10']

message_topic_pub = 'bybp001/sub'
message_topic_sub = 'bybp001/pub'

last_hour_1	 	= 0
actual_hour_1 	= 0
last_hour_2 	= 0
actual_hour_2 	= 0

humidity_trigger_1 = 0
humidity_trigger_2 = 0

co2_trigger_1 = 0
co2_trigger_2 = 0

extractor_max_trigger_1 = False
extractor_max_trigger_2 = False


HUMIDITY_LOWER_TH = 60
HUMIDITY_UPPER_TH = 65
HUMIDITY_MAX_TH = 85
CO2_MAX_TH = 800


def sendMQTTcmd(_msg):
	mqtt_connection.publish(
		topic	= message_topic_pub,
		payload	= _msg,
		qos		= mqtt.QoS.AT_LEAST_ONCE)

def humidifier( room = 1, onoff = True):
	if room == 1:
		if onoff:
			cmd = '{"id": "BYBP001LILS1N003","cmd": "rele","arg": {"ch": 2,"state": 0}}'
		else:
			cmd = '{"id": "BYBP001LILS1N003","cmd": "rele","arg": {"ch": 2,"state": 1}}'
		sendMQTTcmd(cmd)

	elif room == 2:
		if onoff:
			cmd = '{"id": "BYBP001LILS1N008","cmd": "rele","arg": {"ch": 2,"state": 0}}'
		else:
			cmd = '{"id": "BYBP001LILS1N008","cmd": "rele","arg": {"ch": 2,"state": 1}}'
		sendMQTTcmd(cmd)

def extractor(room = 1, ch=1, onoff = True, all = False):
	if room == 1:
		if onoff:
			if all:
				for i in range(3):
					cmd = '{"id": "BYBP001LILS1N002","cmd": "rele","arg": {"ch":'+str(i+1) +' ,"state": 0}}'
					sendMQTTcmd(cmd)
			else:
				cmd = '{"id": "BYBP001LILS1N002","cmd": "rele","arg": {"ch":'+str(ch) +' ,"state": 0}}'
				sendMQTTcmd(cmd)
		else:
			if all:
				for i in range(3):
					cmd = '{"id": "BYBP001LILS1N002","cmd": "rele","arg": {"ch":'+str(i+1) +' ,"state": 1}}'
					sendMQTTcmd(cmd)
			else:
				cmd = '{"id": "BYBP001LILS1N002","cmd": "rele","arg": {"ch":'+str(ch) +' ,"state": 1}}'
				sendMQTTcmd(cmd)

	elif room == 2:
		if onoff:
			if all:
				for i in range(2):
					cmd = '{"id": "BYBP001LILS1N007","cmd": "rele","arg": {"ch":'+str(i+1) +' ,"state": 0}}'
					sendMQTTcmd(cmd)
			else:
				cmd = '{"id": "BYBP001LILS1N007","cmd": "rele","arg": {"ch":'+str(ch) +' ,"state": 0}}'
				sendMQTTcmd(cmd)
		else:
			if all:
				for i in range(2):
					cmd = '{"id": "BYBP001LILS1N007","cmd": "rele","arg": {"ch":'+str(i+1) +' ,"state": 1}}'
					sendMQTTcmd(cmd)
			else:
				cmd = '{"id": "BYBP001LILS1N007","cmd": "rele","arg": {"ch":'+str(ch) +' ,"state": 1}}'
				sendMQTTcmd(cmd)



def updateData(payload):
	#print("Received message {}".format(payload))
	system("clear")
	data = json.loads(payload)
	id = data["id"][-2:]
	print("msj from node : {}".format(id))
	if int(id) in [1,2,5,6,7,9,10,12]:
		t1 = data["sht_t"]
		t2 = data["cap_t"]
		h1 = data["sht_h"]
		h2 = data["cap_h"]
		co2 = data["cap_co2"]
		time_int = data["time"] + 4681443 #corrección manual de la hora
		if int(id) == 9 :
			time_int += 8563
		time = datetime.utcfromtimestamp(time_int).strftime('%H:%M:%S %Y-%m-%d')
		print("new recived values:{},{},{},{},{}. from node {}".format(t1,t2,h1,h2,co2,id))
		sensor_dic[id]["time"] = time
		sensor_dic[id]["t1"] 	= "{:.2f}".format(t1)
		sensor_dic[id]["t2"] 	= "{:.2f}".format(t2)
		sensor_dic[id]["h1"] 	= "{:.2f}".format(h1)
		sensor_dic[id]["h2"] 	= "{:.2f}".format(h2)
		sensor_dic[id]["co2"] 	= "{:.2f}".format(co2)
	if int(id) in [3,8]:
		time_int = data["time"] + 4681443 #corrección manual de la hora
		time = datetime.utcfromtimestamp(time_int).strftime('%H:%M:%S %Y-%m-%d')
		rele = data["relay"]
		humidificador_dic[id]["time"] = time
		humidificador_dic[id]["e"] = "encendido" if ((int(rele)-1)/2 == 1) else "apagado"
	if int(id) in [2,7]:
		time_int = data["time"] + 4681443 #corrección manual de la hora
		time = datetime.utcfromtimestamp(time_int).strftime('%H:%M:%S %Y-%m-%d')
		rele = bin(data["relay"])[2:]
		while len(rele)<3:
			rele = '0'+rele
		print(rele)
		e1 = rele[0]
		e2 = rele[1]
		if int(id) == 2:
			e3 = rele[2]
		extractor_dic[id]["time"] = time
		extractor_dic[id]["e1"] = "encendido" if (e1 == '0') else "apagado"
		extractor_dic[id]["e2"] = "encendido" if (e2 == '0') else "apagado"
		if int(id) == 2:
			extractor_dic[id]["e3"] = "encendido" if (e3 == '0') else "apagado"

	print("---- Sensores ----")
	for e in list(sensor_dic.keys()):
		print("n:{}, time: {},\tt1: {},\tt2: {},\th1: {},\th2: {},\tco2: {}".format(e,sensor_dic[e]["time"],sensor_dic[e]["t1"],sensor_dic[e]["t2"],sensor_dic[e]["h1"],sensor_dic[e]["h2"],sensor_dic[e]["co2"]))
	print("---- Humidificador ----")
	for e in list(humidificador_dic.keys()):
		print("n:{}, time: {},\testado: {}".format(e,humidificador_dic[e]["time"],humidificador_dic[e]["e"]))
	print("---- Extractores ----")
	for e in list(extractor_dic.keys()):
		if int(e) == 2:
			print("n:{}, time: {},\tex1: {},\tex2: {},\tex3: {}".format(e,extractor_dic[e]["time"],extractor_dic[e]["e1"],extractor_dic[e]["e2"],extractor_dic[e]["e3"]))
		else:
			print("n:{}, time: {},\tex1: {},\tex2: {}".format(e,extractor_dic[e]["time"],extractor_dic[e]["e1"],extractor_dic[e]["e2"]))

def updateControl():
	print()
	print("---- Control ----")
	#average all temperatures
	sum_t1 = 0
	sum_h1 = 0
	sum_c1 = 0
	sum_t2 = 0
	sum_h2 = 0
	sum_c2 = 0

	#get sensors from Room 1
	for e in sensors_room_1:
		sum_t1 += float(sensor_dic[e]["t1"])
		sum_h1 += float(sensor_dic[e]["h1"])
		sum_c1 += float(sensor_dic[e]["co2"])

	#calculate average
	avg_t1 = "{:.2f}".format(sum_t1/4.0)
	avg_h1 = "{:.2f}".format(sum_h1/4.0)
	avg_c1 = "{:.2f}".format(sum_c1/4.0)
	print("Sala 1. temp:{}, hum:{}, co2:{}".format(avg_t1,avg_h1,avg_c1))

	#Calculate the status of sensors on room 2
	for e in sensors_room_2:
		sum_t2 += float( sensor_dic[e]["t1"])
		sum_h2 += float(sensor_dic[e]["h1"])
		sum_c2 += float(sensor_dic[e]["co2"])

	avg_t2 = "{:.2f}".format(sum_t2/3.0)
	avg_h2 = "{:.2f}".format(sum_h2/3.0)
	avg_c2 = "{:.2f}".format(sum_c2/3.0)
	print("Sala 2. temp:{}, hum:{}, co2:{}".format(avg_t2,avg_h2,avg_c2))

	#logic to control the state of humidifier of Room 1.
	if float(avg_h1) >= HUMIDITY_UPPER_TH:
		print(" comando apagar humidificador sala 1")
		humidifier(room = 1, onff = True)
	elif float(avg_h1) < HUMIDITY_LOWER_TH:
		print(" comando prender humidificador sala 1")
		humidifier(room = 1, onff = False)

	#logic for humidity control on room 2
	if float(avg_h2) >= HUMIDITY_UPPER_TH:
		print(" comando apagar humidificador sala 2")
		humidifier(room = 2, onff = True)
	elif float(avg_h2) < HUMIDITY_LOWER_TH:
		print(" comando prender humidificador sala 2")
		humidifier(room = 2, onff = False)

	# Logic for extractors on room 1
	for s in sensors_room_1:
 		if (float(sensor_dic[s]["h1"]) > HUMIDITY_MAX_TH) or (float(sensor_dic[s]["h2"]) > HUMIDITY_MAX_TH) or (float(sensor_dic[s]["co2"])> CO2_MAX_TH):
			extractor_max_trigger_1 = True
			break
		else:
			extractor_max_trigger_1 = False

	if extractor_max_trigger_1:# Prender todos los reles
		print(" comando prender todos los extractores sala 1")
		extractor(room = 1, ch = 1, onoff = True, all = True)
	else:
		actual_hour_1 = datetime.now().hour
		if actual_hour_1 != last_hour_1:
			extractor(room = 1, ch = 1, onoff = Flase, all = True) # turn all extrator to off
			if actual_hour_1 < 8:
				print(" comando prender extractor 1 sala 1")
				extractor(room = 1, ch = 1, onoff = True, all = False) # turn on only the extractor n1
			elif actual_hour_1 <= 8  and actual_hour_1 < 16:
				print(" comando prender extractor 2 sala 1")
				extractor(room = 1, ch = 2, onoff = True, all = False) # turn on only the extractor n2
			else:
				print(" comando prender extractor 3 sala 1")
				extractor(room = 1, ch = 3, onoff = True, all = False) # turn on only the extractor n3
			last_hour_1 = actual_hour_1

	# Logic for extractors on room 2
	for s in sensors_room_2:
 		if (float(sensor_dic[s]["h1"]) > HUMIDITY_MAX_TH) or (float(sensor_dic[s]["h2"]) > HUMIDITY_MAX_TH) or (float(sensor_dic[s]["co2"])> CO2_MAX_TH):
			extractor_max_trigger_2 = True
			break
		else:
			extractor_max_trigger_2 = False

	if extractor_max_trigger_2:# Prender todos los reles
		print(" comando prender todos los extractores sala 2")
		extractor(room = 2, ch = 1, onoff = True, all = True)
	else:
		actual_hour_2 = datetime.now().hour
		if actual_hour_2 != last_hour_2:
			extractor(room = 2, ch = 1, onoff = Flase, all = True) # turn all extrator to off
			if actual_hour_1 < 12:
				print(" comando prender extractor 1 sala 2")
				extractor(room = 1, ch = 1, onoff = True, all = False) # turn on only the extractor n1
			else:
				print(" comando prender extractor 2 sala 2")
				extractor(room = 1, ch = 2, onoff = True, all = False) # turn on only the extractor n2
			last_hour_2 = actual_hour_2

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
	print( "new data arrived. updating values")#print("Received message from topic '{}': {}".format(topic, payload))
	updateData(payload)
	global received_count
	received_count += 1
    #if received_count == cmdData.input_count:
    #    received_all_event.set()

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")



if __name__ == '__main__':
	# Create the proxy options if the data is present in cmdData
	proxy_options = None
	#if cmdData.input_proxy_host is not None and cmdData.input_proxy_port != 0:
	#	proxy_options = http.HttpProxyOptions(
	#	host_name=cmdData.input_proxy_host,
	#	port=cmdData.input_proxy_port)

	# Create a MQTT connection from the command line data
	mqtt_connection = mqtt_connection_builder.mtls_from_path(
		endpoint="a3o4b88lasv5ne-ats.iot.sa-east-1.amazonaws.com", #cmdData.input_endpoint,
		port=8883, #cmdData.input_port,
		cert_filepath="credenciales/master_ctrl.pem.crt", #cmdData.input_cert,
		pri_key_filepath="credenciales/master_ctrl-private.pem.key", #cmdData.input_key,
		ca_filepath="credenciales/AmazonRootCA1.pem", #cmdData.input_ca,
		on_connection_interrupted=on_connection_interrupted,
		on_connection_resumed=on_connection_resumed,
		client_id= "Test_MQTT", #cmdData.input_clientId,
		clean_session=False,
		keep_alive_secs=30,
		http_proxy_options=proxy_options,
		on_connection_success=on_connection_success,
		on_connection_failure=on_connection_failure,
		on_connection_closed=on_connection_closed)

	#if not cmdData.input_is_ci:
	#    print(f"Connecting to {cmdData.input_endpoint} with client ID '{cmdData.input_clientId}'...")
	#else:
	#    print("Connecting to endpoint with client ID")
	connect_future = mqtt_connection.connect()

	# Future.result() waits until a result is available
	connect_future.result()
	print("Connected!")

	message_count = 1, #cmdData.input_count
	message_topic = "bybp001/pub" #cmdData.input_topic
	message_string = "" #cmdData.input_message

	# Subscribe
	print("Subscribing to topic '{}'...".format(message_topic))
	subscribe_future, packet_id = mqtt_connection.subscribe(
		topic=message_topic,
		qos=mqtt.QoS.AT_LEAST_ONCE,
		callback=on_message_received)

	subscribe_result = subscribe_future.result()
	print("Subscribed with {}".format(str(subscribe_result['qos'])))

	# Publish message to server desired number of times.
	# This step is skipped if message is blank.
	# This step loops forever if count was set to 0.
	if message_string:
		if message_count == 0:
			print("Sending messages until program killed")
		else:
			print("Sending {} message(s)".format(message_count))

		publish_count = 1
		"""
		while (publish_count <= message_count) or (message_count == 0):
        	message = "{} [{}]".format(message_string, publish_count)
        	print("Publishing message to topic '{}': {}".format(message_topic, message))
        	message_json = json.dumps(message)
        	mqtt_connection.publish(
              	topic=message_topic,
				payload=message_json,
				qos=mqtt.QoS.AT_LEAST_ONCE)
			time.sleep(1)
			publish_count += 1
		"""
    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
	if message_count != 0 and not received_all_event.is_set():
		print("Waiting for all messages to be received...")

    #received_all_event.wait()
    #print("{} message(s) received.".format(received_count))
	try:
		while True:
			time.sleep(60)
			updateControl()
	except KeyboardInterrupt:
		# Disconnect
		print("Disconnecting...")
		disconnect_future = mqtt_connection.disconnect()
		disconnect_future.result()
		print("Disconnected!")
