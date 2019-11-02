#!/usr/bin/env python3
#
# You must first install python-kafka using pip for this to run
# pip install kafka-python
#
# See how to using this library from it official doc
# https://kafka-python.readthedocs.io/en/master/index.html
#

from kafka import KafkaConsumer, KafkaProducer
import json

INPUT_TOPIC = 'toll-raw-stream-topic'
RESPONSE_TOPIC = 'toll-enriched-stream-topic'



VehicleTypeLookup = {
    'BICYCLE' : {'code': 'BICYCLE', 'desc' : '', 'fee' : 0.0},
    'MOTORCYCLE' : {'code': 'MOTORCYCLE', 'desc' : '', 'fee' : 10.0},
    'TRICYCLE' : {'code': 'TRICYCLE', 'desc' : '', 'fee' : 15.0},
    'LIGHTVEHICLE' : {'code': 'LIGHTVEHICLE', 'desc' : '', 'fee' : 50.0},
    'MEDIUMVEHICLE' : {'code': 'MEDIUMVEHICLE', 'desc' : '', 'fee' : 70.0},
    'HEAVYVEHICLE' : {'code': 'HEAVYVEHICLE', 'desc' : '', 'fee' : 90.0},
    'VHEAVYVEHICLE' : {'code': 'VHEAVYVEHICLE', 'desc' : '', 'fee' : 120.0}
}

TollLookup = {
    'toll001' : {'code': 'toll001', 'address' : '', 'location' : [9.006667, 7.263056], 'city' : 'Ikeja', 'state' : 'Lagos'},
    'toll002' : {'code': 'toll002', 'address' : '', 'location' : [6.447250, 3.470260], 'city' : 'Victoria-Island', 'state' : 'Lagos'},
    'toll003' : {'code': 'toll003', 'address' : '', 'location' : [6.412850, 4.087600], 'city' : 'Lekki', 'state' : 'Lagos'},
    'toll004' : {'code': 'toll004', 'address' : '', 'location' : [8.243470, 4.170750], 'city' : 'Ikoyi', 'state' : 'Lagos'},
    'toll005' : {'code': 'toll005', 'address' : '', 'location' : [5.518690, 5.737620], 'city' : 'Warri', 'state' : 'Delta'},
    'toll006' : {'code': 'toll006', 'address' : '', 'location' : [10.531850, 7.429470], 'city' : 'Kaduna', 'state' : 'Kaduna'},
    'toll007' : {'code': 'toll007', 'address' : '', 'location' : [4.815554, 7.049844], 'city' : 'Port Harcourt', 'state' : 'Rivers'},
    'toll008' : {'code': 'toll008', 'address' : '', 'location' : [4.953060, 8.311800], 'city' : 'Calabar', 'state' : 'Cross-Rivers'},
    'toll009' : {'code': 'toll009', 'address' : '', 'location' : [5.037740, 7.912795], 'city' : 'Uyo', 'state' : 'Akwa-Ibom'},
    'toll010' : {'code': 'toll010', 'address' : '', 'location' : [6.132942, 6.792399], 'city' : 'Onitsha', 'state' : 'Anambra'},
    'toll011' : {'code': 'toll011', 'address' : '', 'location' : [9.088196, 7.493382], 'city' : 'Maitama', 'state' : 'Abuja'},
    'toll012' : {'code': 'toll012', 'address' : '', 'location' : [9.311130, 7.239450], 'city' : 'Wuse', 'state' : 'Abuja'}
}

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], acks=1, client_id='transformation-1')

def send_message(msg):
	msg_key = msg['id'].encode('utf8')
	msg_payload = json.dumps(msg).encode('utf8')
	ft = producer.send(RESPONSE_TOPIC, key=msg_key, value=msg_payload)
	try:
	    res_metadata = ft.get(timeout=10)
	    print("Sending :::: %s" % msg_payload)
	except KafkaError as e:
	    print("Error occured %s" % e)


# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(INPUT_TOPIC,
			bootstrap_servers=['localhost:9092'],
			group_id='transformation-module', 
			client_id='transformation-2',
			auto_offset_reset='latest')

for message in consumer:
	# get the message payload
	loaded = json.loads(message.value) 
	# create a new structure
	res = {}
	res['id'] = loaded['id']
	res['vehicle_type'] = VehicleTypeLookup[loaded['vehicle_type']]
	res['total_fee'] = res['vehicle_type']['fee']
	res['toll'] = TollLookup[loaded['toll']]
	res['ts'] = loaded['ts']
	res['payment_type'] = loaded['payment_type']
	if res['payment_type'] == 'CARD':
		res['card_no'] = loaded['card_no']
	else:
		res['card_no'] = ''
	
	#send the new payload
	send_message(res)
