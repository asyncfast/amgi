# Kafka Example

This example includes a docker compose with Kafka (`localhost:9092`) and Kafka UI
([`http://localhost:8080/`](http://localhost:8080/)).

To start services run:

```commandline
docker compose up -d
```

Then run the app with the following command:

```commandline
asyncfast run amgi-aiokafka main:app input_topic
```

You can now send a message on the `input_topic` (You can do this simply in Kafka UI):

```json
{
  "id": "1dde5e4e-c690-49ac-a6c0-d63cf26edef0"
}
```

You should then see the message in the `output_topic`:

```json
{
	"state": "processed",
	"id": "1dde5e4e-c690-49ac-a6c0-d63cf26edef0"
}
```
