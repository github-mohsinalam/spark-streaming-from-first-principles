# Local Kafka Setup Guide

This guide explains how to start the local Kafka environment using the `docker-compose.yml` in this repository and verify that Kafka is working correctly.

The setup uses:

- **Apache Kafka 3.9.1**
- **KRaft mode** — no ZooKeeper
- **Single Kafka broker**
- **Kafka UI** at `http://localhost:8080`
- Kafka host access through `localhost:9092`
- Kafka container-to-container access through `kafka:29092`

---

## 1. Prerequisites

Make sure Docker is installed and running in WSL.

Verify:

```bash
docker --version
docker compose version
```

You should also make sure Docker is running:

```bash
docker ps
```

---

## 2. Navigate to the Kafka Project

The repository already contains the Kafka `docker-compose.yml`.

Navigate to the directory containing the compose file:

```bash
cd /path/to/kafka-project
```

Verify that the file exists:

```bash
ls
```

You should see something similar to:

```text
docker-compose.yml
```

---

## 3. Validate the Docker Compose Configuration

Before starting Kafka, validate the compose configuration:

```bash
docker compose config
```

If there are no errors, the configuration is valid.

---

## 4. Start Kafka

Start Kafka and Kafka UI in detached mode:

```bash
docker compose up -d
```

This will:

1. Pull the required Docker images if they don't already exist.
2. Create the `kafka-data` Docker volume.
3. Start the Kafka container.
4. Wait for Kafka to become healthy.
5. Start the Kafka UI container.

---

## 5. Check Container Status

Run:

```bash
docker compose ps
```

Kafka should eventually show:

```text
Up (healthy)
```

Kafka UI should show:

```text
Up
```

If Kafka is still starting, wait a few seconds and run the command again.

---

## 6. Check Kafka Logs

To follow Kafka startup logs:

```bash
docker compose logs -f kafka
```

Press `Ctrl+C` to stop following the logs.

> `Ctrl+C` only exits the log output. It does **not** stop the Kafka container.

You can also view logs for all services:

```bash
docker compose logs -f
```

---

## 7. Open Kafka UI

Open the following URL in a browser:

```text
http://localhost:8080
```

The Kafka UI should show a cluster named:

```text
local
```

The Kafka UI connects to Kafka using:

```text
kafka:29092
```

because Kafka UI is running inside the Docker network.

---

# 8. Kafka Connection Details

This Docker Compose setup exposes two Kafka listeners.

| Use case | Bootstrap server |
|---|---|
| Applications running on WSL/host | `localhost:9092` |
| Applications running inside Docker | `kafka:29092` |
| Kafka UI | `kafka:29092` |

For example, Spark running outside the Kafka container should use:

```text
localhost:9092
```

while another Docker container should use:

```text
kafka:29092
```

---

# 9. Kafka CLI Commands

The Kafka Docker image places its CLI scripts in `/opt/kafka/bin`.

Instead of repeatedly typing:

```bash
docker exec -it kafka /opt/kafka/bin/<command>
```

you can open a shell inside the Kafka container:

```bash
docker exec -it kafka bash
```

After entering the container, configure the Kafka CLI directory in the `PATH`:

```bash
export PATH="/opt/kafka/bin:$PATH"
```

Now Kafka CLI commands can be run directly:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

The following testing commands assume you have entered the Kafka container and configured the `PATH`.

---

# 10. List Kafka Topics

Run:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

On a new Kafka installation, there may be no user-created topics yet.

---

# 11. Create a Test Topic

Create a topic named `test-topic`:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

Expected output will be similar to:

```text
Created topic test-topic.
```

Verify the topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You should see:

```text
test-topic
```

You can also inspect the topic:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-topic
```

---

# 12. Test Kafka Producer

Start a console producer:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

The terminal will wait for messages.

Type:

```text
hello kafka
```

and press `Enter`.

You can send additional messages if desired:

```text
message 1
message 2
message 3
```

Press `Ctrl+C` to exit the producer.

---

# 13. Test Kafka Consumer

Open a **second WSL terminal**.

Navigate to the Kafka project and enter the Kafka container:

```bash
docker exec -it kafka bash
```

Then configure the Kafka CLI path:

```bash
export PATH="/opt/kafka/bin:$PATH"
```

Start the consumer:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

The messages previously sent by the producer should appear.

For example:

```text
hello kafka
message 1
message 2
message 3
```

This confirms that:

- Kafka is running.
- The topic exists.
- A producer can write messages.
- A consumer can read messages.

Press `Ctrl+C` to exit the consumer.

---

# 14. Optional: Run Kafka CLI Commands Without Entering the Container

If you don't want to open a shell inside the Kafka container, you can run commands directly from WSL.

For example:

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Create a topic:

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

Producer:

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

Consumer:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

This works because the Kafka image makes its CLI scripts available on the container's `PATH`.

---

# 15. Stop Kafka

To stop the Kafka environment:

```bash
docker compose down
```

This stops and removes the containers but **does not remove the Kafka data volume**.

Your topics and messages remain in the `kafka-data` volume.

---

# 16. Start Kafka Again

After it has been stopped:

```bash
docker compose up -d
```

Kafka will start using the existing `kafka-data` volume.

---

# 17. Restart Kafka

If you only need to restart the containers:

```bash
docker compose restart
```

---

# 18. Completely Reset Kafka

If you want to start Kafka from a completely clean state, including deleting all topics and messages:

```bash
docker compose down -v
```

Then:

```bash
docker compose up -d
```

**Warning:** `docker compose down -v` deletes the `kafka-data` volume and therefore deletes all data stored in this local Kafka instance.

Only use this when a complete reset is intended.

---

# 19. Quick Setup

For a new user setting up the repository for the first time:

```bash
cd /path/to/kafka-project

docker compose config

docker compose up -d

docker compose ps

docker compose logs -f kafka
```

Then open:

```text
http://localhost:8080
```

To verify Kafka:

```bash
docker exec -it kafka bash
```

Inside the container:

```bash
export PATH="/opt/kafka/bin:$PATH"

kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create a test topic:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

Start the producer:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

Send:

```text
hello kafka
```

Then from another terminal:

```bash
docker exec -it kafka bash
```

```bash
export PATH="/opt/kafka/bin:$PATH"

kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

You should see:

```text
hello kafka
```

If the message is received successfully, the local Kafka setup is working.
