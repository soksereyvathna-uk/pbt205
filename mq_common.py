import json
import pika

def connect(endpoint="localhost:5672"):
    host, port = endpoint.split(":")
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=int(port))
    )

def declare_topic(channel, name):
    channel.exchange_declare(exchange=name, exchange_type="topic")

def publish(channel, exchange, routing_key, payload):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(payload),
    )

def subscribe(channel, exchange, binding_key, callback):
    queue = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(exchange=exchange, queue=queue, routing_key=binding_key)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
    return queue