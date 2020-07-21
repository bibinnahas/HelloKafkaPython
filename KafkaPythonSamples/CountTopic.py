import os
import sys
import variables
import kafka
from kafka import SimpleClient
from kafka.protocol.offset import offsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload

topic = sys.argv[1]
variables.setVariables()
topic_prefix = 'my_topic_'
table = []
sum_total = []

broker = "%s:%s" % (str(os.environ['KAFKA_BROKER_ADDR']), str(os.environ['KAFKA_BROKER_PORT']))

consumer = kafka.KafkaConsumer(group_id='count_check', bootstrap_Servers=[broker])
client = SimpleClient(broker)

for tpc in table:
    partitions = client.topic_partitions[tpc]
    offset_requests = [OffsetRequestPayload(tpc, p, -1, 1) for p in partitions.keys()]
    offset_responses = client.send_offset_request(offset_requests)
    my_list = []
    for r in offset_responses:
        my_list.append(r.offsets(0))
    sum_total.append(sum(my_list))
    my_list = []
print("%s, %s" % (topic, sum_total))
