# PBT205 Task 1: Chat application

Chat system over RabbitMQ: a command-line client, a GUI client with rooms and history, and automated tests.

## Run
1. Start RabbitMQ (leave this terminal open):
   `docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`
   If you get "container name already in use": `docker rm -f rabbitmq`, then run it again.
2. Install the library: `pip install pika`
3. Command-line chat: `python chat.py USERNAME 5672 [ROOM]`
4. GUI chat: `python chat_gui.py`
5. Automated tests (RabbitMQ must be running): `python test_chat.py`

## Group contract (topic, routing key, JSON fields)
- Topic exchange: `chat`
- Routing key: `room.<roomname>` (room names are lower-cased; spaces and symbols are removed)
- JSON fields: `user`, `room`, `text`, `timestamp` (the command-line client also sends `sid`)
- History: one RabbitMQ stream per room, `history.<roomname>`, bound to the exchange

`mq_common.py` is the group's shared helper module; the chat clients use its `declare_topic` and `publish`. They open their own connections with `heartbeat=0`, because idle connections with the default heartbeat were dropped during testing.
