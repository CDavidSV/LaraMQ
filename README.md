# LaraMQ

LaraMQ is a lightweight, high-performance Java publish/subscribe message broker.

It is named after my cat, Lara.

<img src="content/images/Lara.jpg" alt="Lara the cat" width="320" />

---

## Features

- **Publish / Subscribe** — clients publish messages to named topics; all subscribers receive them in real time.
- **Retained Messages** — a publisher can mark a message as `retain`; new subscribers immediately receive the last retained value for a topic.
- **Undelivered Message Queue** — messages published while a client is offline are queued and flushed automatically on reconnect.
- **Persistent Sessions** — subscriptions and undelivered messages survive broker restarts via JSON-backed storage.
- **Persistent Topics** — topic state (subscribers, retained messages) is persisted to disk and restored on startup.
- **UUID-Based Authentication** — every client is assigned a persistent UUID on first connection; the broker uses it to restore the correct session on reconnect.
- **Analytics** — real-time runtime metrics (connects, disconnects, commands processed, notifications sent).
- **Topic Listing** — query the broker for all currently active topics.
- **Custom Binary Protocol** — compact, length-prefixed frame protocol with typed message codes (ACK, ERROR, NOTIFICATION, AUTHENTICATE).
- **Virtual Threads** — powered by Java 21+ virtual threads for high concurrency with minimal resource overhead.
- **Interactive CLI Client** — REPL-based client with color-coded output and live push notifications.

---

## Protocol

LaraMQ uses a custom binary framing protocol over TCP:

| Field   | Size     | Description                          |
|---------|----------|--------------------------------------|
| Type    | 1 byte   | Message code (ACK, ERROR, etc.)      |
| UUID    | 16 bytes | Request correlation ID (MSB + LSB)   |
| Length  | 4 bytes  | Payload length in bytes              |
| Payload | N bytes  | UTF-8 encoded body (max 10 MB)       |

**Message Codes**

| Code           | Direction        | Description                                                |
|----------------|------------------|------------------------------------------------------------|
| `AUTHENTICATE` | Client → Server  | Client sends its UUID to authenticate on connect           |
| `ACK`          | Server → Client  | Successful command response                                |
| `ERROR`        | Server → Client  | Command or protocol error                                  |
| `NOTIFICATION` | Server → Client  | Real-time message delivery to subscriber                   |
| `COMMAND`      | Client → Server  | Wraps a command; first payload byte is the `CommandCode`   |

After authentication, the server only accepts `COMMAND` frames. The first byte of a `COMMAND` frame's payload identifies the command (subscribe, publish, etc.), and the remaining bytes are the command-specific arguments.

---

## Commands

All commands are sent as UTF-8 text in the frame payload.

| Command                            | Description                                                        |
|------------------------------------|--------------------------------------------------------------------|
| `subscribe <topic>`                | Subscribe to a topic. Receives the retained message if one exists. |
| `unsubscribe <topic>`              | Unsubscribe from a topic.                                          |
| `publish <topic> <message>`        | Publish a message to a topic.                                      |
| `publish <topic> <message> retain` | Publish and retain the message for future subscribers.             |
| `list`                             | List all active topics on the broker.                              |
| `analytics`                        | Retrieve a JSON snapshot of broker runtime metrics.                |
| `exit`                             | Disconnect from the broker.                                        |

---

## Authentication & Session Management

1. On the **first connection**, the broker generates a UUID and sends it to the client via an `AUTHENTICATE` frame. The client stores this UUID locally in `data/client_config.json`.
2. On **subsequent connections**, the client sends its stored UUID, and the broker restores the associated session (subscriptions + undelivered messages).
3. Undelivered messages (published while the client was offline) are **automatically flushed** to the client upon reconnect.

---

## Persistence

| Data               | File                        | Description                                         |
|--------------------|-----------------------------|-----------------------------------------------------|
| Topic state        | `data/topic_data.json`      | Retained messages and subscriber lists per topic    |
| Client sessions    | `data/client_sessions.json` | Per-client subscriptions and undelivered queues     |
| Client identity    | `data/client_config.json`   | Locally stored UUID for session restoration         |

---

## Analytics

The `analytics` command returns a JSON object with live broker statistics:

```json
{
  "totalConnects": 42,
  "totalDisconnects": 10,
  "totalCommandsProcessed": 198,
  "totalNotificationsSent": 87
}
```

---

## Build first (required for local runs)

To run the server or client directly with Java, build the project first:

```bash
mvn clean package -DskipTests
```

This creates the runnable JAR at `target/LaraMQ.jar`.

---

## Run the server locally

```bash
java -jar target/LaraMQ.jar
```

The broker listens on `127.0.0.1:3000` by default.

---

## Run the server with Docker

```bash
docker build -t laramq .
docker run --rm -p 3000:3000 --name laramq-server laramq
```

The container starts the broker on `0.0.0.0:3000`.

---

## Run the client

In another terminal, from the project root:

```bash
java -cp target/LaraMQ.jar client.LaraMQClient
```

---

## Quick command examples

```text
subscribe weather
publish weather sunny retain
publish weather rainy
list
analytics
unsubscribe weather
exit
```