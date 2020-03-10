# kafkaTestScripts

1. Those scripts run on the Docker container based Kafka testbed. Therefore the precondition is that Kafka cluster is succefully build. Details can be found in our "kafka_start_up" repository. 

2. The scripts should be mounted to every Kafka producer or consumer node before running. The target file path in each container should be "/home/apps/deliveryTest/".

3. The file paths in the scripts (autoTest1.py and resultAnalysis.py) need to be modified depending on your local file paths.

4. Once the testbed is successfully build, run autoTest1.py to run tests and generate testing results.


