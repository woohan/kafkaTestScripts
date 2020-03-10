# analyze the results of duplicate, lost messages
from termcolor import colored
import time


startTime = time.time()
totalMsgNum = 1000000
fo = open('/YOURPATH/deliveryTest/recordData/receivedIDs')

sum_expect = totalMsgNum # the number of expected messages received

v1s = fo.readlines() # all messages' ids received in consumer
sum_v1s = len(v1s) # number of received messages
v1_actual = set(v1s) # all nonidentical messages
sum_actual = len(v1_actual) # the actual number of nonidentical messages
sum_lost = sum_expect - sum_actual # the number of lost messages
per_lost = sum_lost/sum_expect # the percentage of lost messages
v1s_redundant = v1s # initializing all redundant messages
for x in v1_actual :
    v1s_redundant.remove(x)
sum_redundant = len(v1s_redundant) # the number of redundant messages
per_redundant = sum_redundant/sum_v1s # the percentage of redundant msgs
sum_duplicate = len(set(v1s_redundant)) # the number of duplicated messages
per_duplicate = sum_duplicate/sum_expect # the percentage of duplicated msgs

res_lst = map(str,  [sum_v1s,
        sum_actual,  sum_lost,
        per_lost, sum_redundant, per_redundant, sum_duplicate,
        per_duplicate])
res_str = ",".join(res_lst)

fo.close()

endTime = time.time()

print('Total time cost: ', endTime-startTime)
print('Result analysis:-----------------------------------------------------')
print('The total number of messages received is', colored(len(v1s), 'yellow'))
print('The number of all different messages is', colored(sum_actual, 'magenta'))
print('The number of lost messages is', colored(sum_lost, 'red'), ', lost percentage:', colored(per_lost*100, 'red'), '%')
print('The number of duplicated messages is', colored(sum_duplicate, 'cyan'), ', duplicated percentage:', colored(per_duplicate*100, 'cyan'),'%')
print('---------------------------------------------------------------------')