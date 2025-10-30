# Example

Example is meant to demonstrate the portability of AMGI applications.

The application consumes messages from the `input_channel` in the form:

```json
{
  "id": "1dde5e4e-c690-49ac-a6c0-d63cf26edef0"
}
```

This example includes mult

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
docker compose --file kafka/docker-compose.yml up --detach
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
docker compose --file mqtt/docker-compose.yml up --detach
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
