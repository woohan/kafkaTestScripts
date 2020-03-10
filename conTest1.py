# consumer api kfk3
import logging
import sys
import time
import uuid
import bitarray
import numpy as np
from confluent_kafka import Consumer, KafkaError
import json

totalMsgNum = 1000000 # total number of messages sent from producer
recordFileName = '1106_64mbuff_poll0_msgtmout3000.csv' # file that records the results
logFileName = '1106_64mbuff_poll0_msgtmout3000' # log file

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

def run_consumer():
    topicName = sys.argv[1]

    consumer = Consumer({'bootstrap.servers':'172.23.0.3:9092,172.23.0.4:9092,172.23.0.2:9092', 
        'group.id': str(uuid.uuid1()),
        'auto.offset.reset': 'earliest',})
    consumer.subscribe([topicName])
    # store received message ids in a file for analysis
    f1 = open('/home/apps/deliveryTest/recordData/receivedIDs', 'w')

    startTime = time.time()
    receivedNum = 0 # initializing total number of received messages
    while True:
        msg = consumer.poll(10)
        if msg is None:
            logger2.info("No more messages to consume now")
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(msg.error())
                break

        msg_id = msg.value().decode('utf-8').split(',')[0]
        # logger1.debug(msg_id+'received')
        receivedNum+=1 # count the number of received messages
        f1.write(msg_id+'\n')

    f1.close()
    consumer.close()
    endTime = time.time()
    totalTime = endTime-startTime

    logger2.info('The total time to receive '+str(receivedNum)+' messages from topic is '+str(totalTime)+' seconds')
    logger2.info('The average time for each message is '+str(totalTime*1000/receivedNum)+' ms')

def analysis():
    logger2.info("Analyzing...") # analyze the results of duplicate, lost messages
    topicName = sys.argv[1]

    v1s = np.loadtxt('/home/apps/deliveryTest/recordData/receivedIDs', dtype='int')
    with open('/home/apps/deliveryTest/parameters.json') as fp:
        paras = json.load(fp)
    delay = paras['network.delay']
    pktloss = paras['packet.loss']
    bufferKb = paras['queue.buffering.max.kbytes']
    bufferMs = paras['queue.buffering.max.ms']
    msgSize = paras['message.size']
    batchSize = paras['batch.num.messages']
    msgTmout = paras['default.topic.config']['message.timeout.ms']
    acks = paras['default.topic.config']['request.required.acks']
    retryNum = paras['message.send.max.retries']
    retryMs = paras['retry.backoff.ms']
    proTime = paras['total.produce.time']

    duplicate = {} # key-->msgId, value-->times of appearance
    miss = [] # Lists of lost messages
    ba = bitarray.bitarray(10**6+1000) # bit array to store value of received v1s
    ba.setall(False)
    for id in v1s:
        duplicate.setdefault(id, 1) # if id not exist, set value as 1, else nothing
        if ba[id]:
            duplicate[id] += 1 # if exist, value+1
        ba[id] = True
    sum_expect = totalMsgNum # the number of expected messages received
    for i in range(sum_expect):
        if not ba[i]:
            miss.append(i)
    sum_v1s = len(v1s) # number of received messages
    sum_actual = len(duplicate) # the actual number of nonidentical messages
    sum_lost = len(miss) # the number of lost messages
    per_lost = sum_lost/sum_expect # the percentage of lost messages
    sum_redundant = 0
    sum_duplicate = 0
    for i in duplicate:
        if duplicate[i] == 1:
            continue
        else:
            sum_redundant+=(duplicate[i]-1)
            sum_duplicate+=1
    per_redundant = sum_redundant/sum_v1s # the percentage of redundant msgs
    per_duplicate = sum_duplicate/sum_expect # the percentage of duplicated msgs

    res_lst = map(str,  [delay, pktloss, bufferKb, bufferMs, msgSize, batchSize, msgTmout, acks, retryNum, retryMs, 
            sum_v1s, sum_actual, sum_lost,
            per_lost, sum_redundant, per_redundant, sum_duplicate,
            per_duplicate,
            proTime])
    res_str = ",".join(res_lst)
    logger2.info('Result is: \n'+res_str)
    with open('/home/apps/deliveryTest/recordData/'+recordFileName, "a") as fw:
        fw.write(res_str+"\n")

    logger2.info("Analysis finished")


run_consumer()

analysis()

