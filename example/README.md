# Example

Example is meant to demonstrate the portability of AMGI applications.

The application consumes messages from the `input_channel` in the form:

```json
{
  "id": "1dde5e4e-c690-49ac-a6c0-d63cf26edef0"
}
```

And sends messages to `output_channel` in the form:

```json
{
  "state": "processed",
  "id": "1dde5e4e-c690-49ac-a6c0-d63cf26edef0"
}
```

This example includes multiple docker compose files for different protocols, which you can then run the application against.

## Kafka

Run the Kafka compose file with:

```commandline
docker compose --file kafka/docker-compose.yaml up --detach
```

This includes:

- A broker running on `localhost:9092`
- Kafka UI Kafka UI ([`http://localhost:8080/`](http://localhost:8080/))

Connect the app via Kafka with:

```commandline
asyncfast run amgi-aiokafka main:app input_channel
```

## MQTT

Run the MQTT compose file with:

```commandline
docker compose --file mqtt/docker-compose.yaml up --detach
```

This includes:

- An Eclipse Mosquitto server running two listeners
  - MQTT listening on `localhost:1883`
  - WebSocket running on `localhost:9001`
- MQTTX Web browser client ([`http://localhost:8081/`](http://localhost:8081/))

Connect the app via MQTT with:

```commandline
asyncfast run amgi-paho-mqtt main:app input_channel
```

## SQS

Run the SQS compose file with:

```commandline
docker compose --file sqs/docker-compose.yaml up --detach
```

This includes:

- Localstack to emulate AWS `localhost:4566`
- SQS Admin UI ([`http://localhost:3999/`](http://localhost:3999/))

You can then use SQS Admin UI to create the two queues `input_channel`, and `output_channel`.

Connect the app via SQS with:

```commandline
asyncfast run amgi-aiobotocore-sqs main:app input_channel --region-name us-west-1 --endpoint-url http://localhost:4566 --aws-access-key-id localstack --aws-secret-access-key localstack
```

## Redis

Run the redis compose file with:

```commandline
docker compose --file redis/docker-compose.yaml up --detach
```

This includes:

- Redis `redis://localhost:6379/`
- Redis Insight ([`http://localhost:5540/`](http://localhost:5540/))

Connect the app via Redis with:

```commandline
asyncfast run amgi-redis main:app input_channel
```

## AMQP

Run the AMQP compose file with:

```commandline
docker compose --file amqp/docker-compose.yaml up --detach
```

This includes:

- RabbitMQ `amqp://guest:guest@localhost:5672/`
- RabbitMQ Management UI ([`http://localhost:15672/`](http://localhost:15672/)) (username: `guest`, password: `guest`)

Connect the app via AMQP with:

```commandline
asyncfast run amgi-aiopika-amqp main:app input_channel
```
