"""
Task 1 - Command-line chat app with chat history (PBT205)

Usage:   python chat.py <username> <port> [room]
Example: python chat.py alice 5672
         python chat.py bob 5672 study

Design notes
- One topic exchange "chat" (group contract); the routing key is "room.<name>".
- Each room has a RabbitMQ stream "history.<room>" bound to that routing key.
  Streams are non-destructive logs, so a user who joins later reads the room's
  earlier messages first, then keeps receiving new ones live.
- This file and chat_gui.py use the same exchange and streams, so CLI and GUI
  users can chat in the same room.
"""
import re
import sys
import json
import time
import uuid
import threading

import pika

from mq_common import declare_topic, publish  

EXCHANGE = "chat"
HOST = "localhost"
DEFAULT_ROOM = "room"
GO = threading.Event()   # set by main() after it prints the 'Joined' line


def clean_room(name):
    """Lower-case and remove spaces/special characters so 'PBT 205' and 'pbt205' match."""
    return re.sub(r"[^a-z0-9_-]", "", name.lower())


def listen(port, room, username, sid, ready, status):
    """Background thread: read the room's stream (history first, then live)."""
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(HOST, port, heartbeat=0))
        ch = conn.channel()
        declare_topic(ch, EXCHANGE)

        stream = f"history.{room}"
        ch.queue_declare(queue=stream, durable=True,
        arguments={"x-queue-type": "stream", "x-max-age": "7D"})
        ch.queue_bind(exchange=EXCHANGE, queue=stream, routing_key=f"room.{room}")
        ch.basic_qos(prefetch_count=100)   # required for streams
    except Exception as e:
        status["error"] = str(e)
        ready.set()
        return

    today = time.strftime("%Y-%m-%d")

    def on_message(channel, method, props, body):
        msg = json.loads(body)
        channel.basic_ack(method.delivery_tag)       # streams require acks
        if msg.get("sid") == sid:                     # message sent by THIS session:
            return                                    # already visible in the terminal
        t = msg.get("timestamp", time.time())
        fmt = "%H:%M" if time.strftime("%Y-%m-%d", time.localtime(t)) == today else "%d %b %H:%M"
        who = "You" if msg["user"] == username else msg["user"]
        print(f"\r[{time.strftime(fmt, time.localtime(t))}] {who}: {msg['text']}\n> ",
        end="", flush=True)

    # offset "first" = start from the oldest stored message, then continue live
    ch.basic_consume(queue=stream, on_message_callback=on_message, auto_ack=False,
    arguments={"x-stream-offset": "first"})
    ready.set()
    GO.wait()   # history is printed only after the 'Joined' line is shown
    try:
        ch.start_consuming()
    except Exception as e:
        print(f"\n* Connection to RabbitMQ lost ({type(e).__name__}). Restart to rejoin.")


def main():
    if len(sys.argv) < 3:
        print("Usage: python chat.py <username> <port> [room]")
        sys.exit(1)

    username = sys.argv[1]
    try:
        port = int(sys.argv[2])
    except ValueError:
        print("Port must be a number, e.g. 5672.")
        sys.exit(1)
    raw_room = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_ROOM
    room = clean_room(raw_room)
    if not room:
        print("Room name must contain letters, numbers, - or _.")
        sys.exit(1)
    if room != raw_room.lower():
        print(f"* Room name cleaned to '{room}' (spaces and special characters removed).")

    sid = uuid.uuid4().hex   # identifies this session so we don't print our own live messages

    # Start subscriber first (it also loads the history)
    ready = threading.Event()
    status = {}
    threading.Thread(target=listen, args=(port, room, username, sid, ready, status),
    daemon=True).start()
    ready.wait(10)
    if "error" in status or not ready.is_set():
        print(f"Could not connect to RabbitMQ on port {port}. Is Docker running?\n"        
        f"{status.get('error', 'Timed out')}")
        sys.exit(1)

    # Separate connection for publishing (heartbeat=0: it sits idle while we wait for typing)
    conn = pika.BlockingConnection(pika.ConnectionParameters(HOST, port, heartbeat=0))
    ch = conn.channel()
    declare_topic(ch, EXCHANGE)

    print(f"Joined room '{room}' as {username}. Type a message and press Enter. Ctrl+C to quit.")
    GO.set()   # now let the listener print the room history
    try:
        while True:
            text = input("> ").strip()
            if not text:
                continue
            publish(ch, EXCHANGE, f"room.{room}", {
                "user": username,
                "room": room,
                "text": text,
                "timestamp": time.time(),
                "sid": sid,
            })
    except (KeyboardInterrupt, EOFError):
        print("\nLeaving chat.")
    except Exception as e:
        print(f"\nSend failed: {e}")
    finally:
        if conn.is_open:
            conn.close()


if __name__ == "__main__":
    main()
