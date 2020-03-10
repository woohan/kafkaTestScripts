# automatically run tests on Kafka kfk
import uuid
import os
import numpy as np
import subprocess
from time import sleep
import logging
import sys

# delay/packet loss rate of out going packages
# impacts = np.arange(0.5, 25.5, 0.5)
impacts = np.arange(30,230,50)
# impacts = [0, 0, 0]
logFileName = '1106_64mbuff_poll0_msgtmout3000'

#-----------------------Logger1 only outputs to file-----------------------#
formatter = logging.Formatter('%(asctime)s-[%(levelname)s]-{%(filename)s-%(lineno)d} - %(message)s')
# Time, log level, filename and line, value
logger1 = logging.getLogger("logger1")
hd1 = logging.FileHandler("/YOURPATH/deliveryTest/recordData/logs/"+logFileName+".log")
hd1.setFormatter(formatter)
logger1.addHandler(hd1)
logger1.setLevel(logging.INFO)  # Logger1 Level

logger2 = logging.getLogger("logger2")
hd2 = logging.FileHandler("/YOURPATH/deliveryTest/recordData/logs/"+logFileName+".log")
hd2.setFormatter(formatter)
logger2.addHandler(hd1)
logger2.addHandler(logging.StreamHandler(sys.stdout))
logger2.setLevel(logging.INFO)  # Logger2 Level
#--------------Logger2 outputs to both file&console---------------------#

class netEmulate:
    def __init__(self, containers, delay, packetloss):
        self.containers = containers
        self.delay = delay
        self.packetloss = packetloss
    def startCmd(self):
        if self.packetloss == '0%':
            for c in self.containers:
                subprocess.run(['docker', 'exec', '-it', c, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'delay', self.delay])
        else:
            for c in self.containers:
                subprocess.run(['docker', 'exec', '-it', c, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'delay', self.delay, 'loss', self.packetloss])
    def stopCmd(self):
        for c in self.containers:
            subprocess.run(['docker', 'exec', '-it', c, 'tc', 'qdisc', 'delete', 'dev', 'eth0', 'root'])

def runImpactsTests(impacts, impacts2):
    for i in impacts:
        topic_name = str(uuid.uuid4()).replace("-", "")
        del_topic_cmd = ["docker",
            "exec", "kfk_pro_1",
            "/home/apps/deliveryTest/deleteTopic.sh",
            topic_name]
        create_topic_cmd =["docker",
            "exec", "kfk_pro_1",
            "/home/apps/deliveryTest/createTopic.sh",
            "3", "1", topic_name]
        halfDelay = i
        pktloss = 0
        delayStr = str(halfDelay)+'ms'
        pktlossStr = str(pktloss)+'%'
        p1 = netEmulate(['kfk_pro_1', 'kfk_node_1', 'kfk_node_2', 'kfk_node_3'], delayStr, pktlossStr)
        con_cmd = ["docker",
            "exec", "kfk_con_1", "python3",
            "/home/apps/deliveryTest/conTest1.py", topic_name]
        pro_cmd = ["docker",
            "exec", "kfk_pro_1", "python3",
            "/home/apps/deliveryTest/proTest1.py", topic_name, str(halfDelay*2), str(pktloss), str(impacts2)]
        subprocess.run(create_topic_cmd)
        logger2.info('Created Topic: '+topic_name)
        
        p1.startCmd()
        sleep(2)
        logger2.info('Network Fault Injected ...')
        logger2.info('Starting Producer ...')
        pro_process = subprocess.Popen(pro_cmd)
        pro_process.wait()
        p1.stopCmd()
        logger2.info('Network Fault stopped')
        logger2.info('Starting Consumer ...')
        con_process = subprocess.Popen(con_cmd)
        con_process.wait()
    
        subprocess.run(del_topic_cmd)
        logger2.info('Topic deleted')

batches = np.arange(10,11,1)
for impacts2 in batches:
    runImpactsTests(impacts, impacts2)


