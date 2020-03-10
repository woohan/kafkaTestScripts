# producer api
import os
from confluent_kafka import Producer
import numpy as np
import subprocess
import strgen
import sys
import time
import logging
import json

totalMsgNum = 1000000
logFileName = '1106_64mbuff_poll0_msgtmout3000'

#-----------------------Logger1 only outputs to file-----------------------#
formatter = logging.Formatter('%(asctime)s-[%(levelname)s]-{%(filename)s-%(lineno)d} - %(message)s')
# Time, log level, filename and line, value
logger1 = logging.getLogger("logger1")
hd1 = logging.FileHandler("/home/apps/deliveryTest/recordData/logs/"+logFileName+".log")
hd1.setFormatter(formatter)
logger1.addHandler(hd1)
logger1.setLevel(logging.DEBUG)  # Logger1 Level

logger2 = logging.getLogger("logger2")
hd2 = logging.FileHandler("/home/apps/deliveryTest/recordData/logs/"+logFileName+".log")
hd2.setFormatter(formatter)
logger2.addHandler(hd1)
logger2.addHandler(logging.StreamHandler(sys.stdout))
logger2.setLevel(logging.DEBUG)  # Logger2 Level
#--------------Logger2 outputs to both file&console---------------------#


# TODO maybe we can change IPs automatically...
proConf = {'bootstrap.servers':'172.23.0.3:9092,172.23.0.4:9092,172.23.0.2:9092',
    'queue.buffering.max.kbytes': 65536, # kibibyte
    'queue.buffering.max.ms': 0.5,
    'batch.num.messages': int(sys.argv[4]),
    'message.send.max.retries': 0,
    'retry.backoff.ms': 40,
    #'stats_cb': my_stats_callback,
    #'statistics.interval.ms': 100,
    'default.topic.config': {'message.timeout.ms': 3000, 'request.required.acks': 0 }}

producer = Producer(proConf)
msg = ''
topicName = sys.argv[1]

def send_report(err, msg):
    if err:
        logger1.debug(err)

startTime = time.time()
for i in range(totalMsgNum):
# generate sequential and continues numbers as the first part of each message
    v1 = '{0:07}'.format(i)
# generate a message with certain bits
    v2Bit = str(45) # bit
    v2 = strgen.StringGenerator("[\d\w]{"+v2Bit+"}").render()
    msg = str(v1+','+v2)
    try:
        producer.poll(0)
        producer.produce(topicName, bytes(msg, 'utf-8'),callback=send_report)
    except BufferError as bfer:
        logger2.error(bfer)
        producer.poll(0)
    # logger1.debug(v1+'sent')
    # if i%100==0:
    #     time.sleep(0.001)

producer.flush()
endTime = time.time()

del proConf['bootstrap.servers']
proConf['message.size'] = sys.getsizeof(msg)
proConf['network.delay'] = float(sys.argv[2])
proConf['packet.loss'] = float(sys.argv[3])
proConf['total.produce.time'] = endTime-startTime
with open('/home/apps/deliveryTest/parameters.json', "w") as fw:
    json.dump(proConf, fw)

logger2.info('Total time for sending messages to topic is '+ str(endTime-startTime)+' seconds')
logger2.info('The size of each message is '+str(sys.getsizeof(msg))+' bytes')


# def set_done():
#     os.system("export pro_done=\"1\"")
#     print('producer done')


