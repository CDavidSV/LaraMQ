# LaraMQ

LaraMQ is a lightweight Java publish/subscribe broker.

## Build first (required for local runs)

To run the server or client directly with Java, build the project first:

```bash
mvn clean package -DskipTests
```

This creates the runnable JAR at `target/LaraMQ.jar`.

### Run server locally

```bash
java -jar target/LaraMQ.jar
```

## How it works

- The broker listens for TCP clients (default: `127.0.0.1:3000`).
- Clients can `subscribe`, `unsubscribe`, and `publish` messages per topic.
- A published message can be retained (`retain`) so new subscribers receive the latest value.
- The broker also exposes an `analytics` command for runtime stats.

## Run the server with Docker

From the project root:

```bash
docker build -t laramq .
docker run --rm -p 3000:3000 --name laramq-server laramq
```

The container starts the broker on `0.0.0.0:3000`.

## Run the client

In another terminal, from the project root:

```bash
java -cp target/LaraMQ.jar client.LaraMQClient
```

## Quick command examples

```text
subscribe weather
publish weather sunny retain
analytics
unsubscribe weather
exit
```

