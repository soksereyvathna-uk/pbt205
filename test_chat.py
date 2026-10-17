"""
Task 1 - Automated tests for the chat system (PBT205)

Run (RabbitMQ must be running):   python test_chat.py
Optional port:                    python test_chat.py 5672

Each test uses a unique random room name, so it never interferes with real chats.
Tests talk to RabbitMQ the same way chat.py / chat_gui.py do.
"""
import sys
import json
import time
import uuid
import threading

import pika

from chat import clean_room   # reuse the real room-name cleaning function

EXCHANGE = "chat"
HOST = "localhost"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5672
created_streams = []


# ---------- helpers (same setup as the real apps) ----------
def connect(port=PORT):
    return pika.BlockingConnection(pika.ConnectionParameters(HOST, port, heartbeat=0))


def setup_room(ch, room):
    """Declare exchange + the room's history stream + binding (same as the apps)."""
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
    stream = f"history.{room}"
    ch.queue_declare(queue=stream, durable=True,
    arguments={"x-queue-type": "stream", "x-max-age": "7D"})
    ch.queue_bind(exchange=EXCHANGE, queue=stream, routing_key=f"room.{room}")
    if stream not in created_streams:
        created_streams.append(stream)
    return stream


def publish(room, user, text):
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
    body = json.dumps({"user": user, "room": room, "text": text, "timestamp": time.time()})
    ch.basic_publish(exchange=EXCHANGE, routing_key=f"room.{room}", body=body)
    conn.close()


def read_room(room, idle=2.0):
    """Join a room like a late joiner: read from the oldest message until quiet."""
    conn = connect()
    ch = conn.channel()
    stream = setup_room(ch, room)
    ch.basic_qos(prefetch_count=100)
    messages = []
    for method, props, body in ch.consume(queue=stream, auto_ack=False,
        arguments={"x-stream-offset": "first"},
        inactivity_timeout=idle):
        if method is None:        # nothing new for `idle` seconds
            break
        messages.append(json.loads(body))
        ch.basic_ack(method.delivery_tag)
    ch.cancel()
    conn.close()
    return messages


def new_room():
    return "test" + uuid.uuid4().hex[:8]


# ---------- tests ----------
def test_connection():
    """Can connect to RabbitMQ and open a channel"""
    conn = connect()
    assert conn.is_open
    conn.channel()
    conn.close()


def test_message_delivery():
    """A message sent to a room is received with correct user and text"""
    room = new_room()
    conn = connect()
    setup_room(conn.channel(), room)
    conn.close()
    publish(room, "alice", "hello bob")
    msgs = read_room(room)
    assert len(msgs) == 1, f"expected 1 message, got {len(msgs)}"
    assert msgs[0]["user"] == "alice" and msgs[0]["text"] == "hello bob"


def test_message_format():
    """Messages are JSON with user, room, text and timestamp"""
    room = new_room()
    conn = connect()
    setup_room(conn.channel(), room)
    conn.close()
    publish(room, "alice", "format check")
    m = read_room(room)[0]
    for key in ("user", "room", "text", "timestamp"):
        assert key in m, f"missing field '{key}'"
    assert m["room"] == room


def test_live_delivery():
    """A user already in the room receives a new message live"""
    room = new_room()
    conn = connect()
    ch = conn.channel()
    stream = setup_room(ch, room)
    ch.basic_qos(prefetch_count=100)
    threading.Timer(1.0, publish, args=(room, "carol", "live message")).start()
    got = None
    for method, props, body in ch.consume(queue=stream, auto_ack=False,
        arguments={"x-stream-offset": "next"},
        inactivity_timeout=5):
        if method is None:
            break
        got = json.loads(body)
        ch.basic_ack(method.delivery_tag)
        break
    ch.cancel()
    conn.close()
    assert got is not None, "no live message received within 5 seconds"
    assert got["text"] == "live message"


def test_room_isolation():
    """Messages in one room are NOT seen in another room"""
    room_a, room_b = new_room(), new_room()
    conn = connect()
    ch = conn.channel()
    setup_room(ch, room_a)
    setup_room(ch, room_b)
    conn.close()
    publish(room_a, "alice", "only for room A")
    publish(room_b, "bob", "only for room B")
    texts_a = [m["text"] for m in read_room(room_a)]
    texts_b = [m["text"] for m in read_room(room_b)]
    assert texts_a == ["only for room A"], f"room A got {texts_a}"
    assert texts_b == ["only for room B"], f"room B got {texts_b}"


def test_history_for_late_joiner():
    """A user who joins later receives earlier messages, in order"""
    room = new_room()
    conn = connect()
    setup_room(conn.channel(), room)       # first user opens the room
    conn.close()
    for i in range(1, 4):
        publish(room, "alice", f"message {i}")
    late = read_room(room)                 # late joiner arrives afterwards
    texts = [m["text"] for m in late]
    assert texts == ["message 1", "message 2", "message 3"], f"got {texts}"


def test_history_for_multiple_late_joiners():
    """History is not used up: a second late joiner also sees everything"""
    room = new_room()
    conn = connect()
    setup_room(conn.channel(), room)
    conn.close()
    publish(room, "alice", "one")
    publish(room, "bob", "two")
    first = [m["text"] for m in read_room(room)]
    second = [m["text"] for m in read_room(room)]
    assert first == second == ["one", "two"], f"first={first} second={second}"


def test_room_name_cleaning():
    """Room names are cleaned: 'PBT 205' and 'pbt205' are the same room"""
    cases = {
        "PBT 205": "pbt205",
        "General": "general",
        "  study  ": "study",
        "my room!": "myroom",
        "a_b-c": "a_b-c",
    }
    for raw, expected in cases.items():
        assert clean_room(raw) == expected, f"{raw!r} -> {clean_room(raw)!r}, expected {expected!r}"


def test_connection_failure():
    """Connecting to a wrong port fails cleanly (error handling)"""
    try:
        pika.BlockingConnection(pika.ConnectionParameters(
            HOST, 5999, connection_attempts=1, socket_timeout=2))
    except pika.exceptions.AMQPConnectionError:
        return
    raise AssertionError("expected a connection error but it connected")


TESTS = [
    test_connection,
    test_message_delivery,
    test_message_format,
    test_live_delivery,
    test_room_isolation,
    test_history_for_late_joiner,
    test_history_for_multiple_late_joiners,
    test_room_name_cleaning,
    test_connection_failure,
]


def cleanup():
    try:
        conn = connect()
        ch = conn.channel()
        for stream in created_streams:
            ch.queue_delete(queue=stream)
        conn.close()
    except Exception:
        pass


def main():
    print(f"Chat system tests (RabbitMQ on port {PORT})")
    print("=" * 72)
    passed = 0
    for n, test in enumerate(TESTS, 1):
        name = test.__doc__.strip()
        try:
            test()
            result, detail = "PASS", ""
            passed += 1
        except Exception as e:
            result, detail = "FAIL", f"  -> {type(e).__name__}: {e}"
        print(f"{n:>2}. [{result}] {name}{detail}")
    cleanup()
    print("=" * 72)
    print(f"{passed}/{len(TESTS)} tests passed")
    sys.exit(0 if passed == len(TESTS) else 1)


if __name__ == "__main__":
    main()
