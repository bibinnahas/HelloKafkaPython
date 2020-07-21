import sys
import os
import variables
from termcolor import colored, cprint
from confluent_kafka import Producer, Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

variables.setVariables()

url = "http://%s:%s" % (str(os.environ['KAFKA_SCHEMA_ADDR']), str(os.environ['KAFKA_SCHEMA_PORT']))
broker = "%s:%s" % (str(os.environ['KAFKA_BROKER_ADDR']), str(os.environ['KAFKA_BROKER_PORT']))

if not 1 < len(sys.argv) < 3:
    cprint("Error", 'red', attrs=['bold'], file=sys.stderr)
    raise TypeError('Require an argument. Supplied {}'.format(len(sys.argv) - 1))

with open("list") as f:
    content = f.readlines()
content = [x.strip() for x in content]


def delivery_report(error, message):
    """Triggered by poll or FLush"""
    if error is not None:
        print('Message not delivered: {}'.format(error))
    else:
        print('Message Delivered to {}[{}]'.format(message.topic(), message.partition))


consumer = AvroConsumer({'bootstrap.servers': broker,
                         'group_id': [sys.argv[1]],
                         'schema.registry_url': url,
                         'default.topic.config': {'auto.offset.reset': 'earliest'}
                         })
consumer.subscribe(content)
producer = Producer({'bootstrap.servers': broker,
                     'enable.idempotence': True})

while True:
    try:
        cprint("Subscribing..", 'green', attrs=['bold'], file=sys.stderr)
        message = consumer.poll(10)
    except SerializerError as e:
        cprint("Error", 'red', attrs=['bold'], file=sys.stderr)
        break
    if message is None:
        continue
    else:
        producer.produce(str(message.topic()), str(message.value()), callback=delivery_report)

consumer.close()
