# AMGI

[![Tests](https://github.com/asyncfast/amgi/actions/workflows/tests.yml/badge.svg)](https://github.com/asyncfast/amgi/actions/workflows/tests.yml)

AMGI (*Asynchronous Messaging Gateway Interface*) is the spiritual sibling of
[ASGI](https://asgi.readthedocs.io/en/latest/). While the focus of [ASGI](https://asgi.readthedocs.io/en/latest/) is
HTTP, the focus of AMGI is event-based applications.

This repository contains multiple AMGI implementations, as well as a typed microframework, AsyncFast, in the spirit of
[FastAPI](https://fastapi.tiangolo.com/) it attempts to make typed asynchronous APIs easy to develop.

## Core Aims

Core aims of both AMGI and AsyncFast:

- **Portable**: Following [AMGI](https://amgi.readthedocs.io/en/latest/) should allow for implementations of any
  protocol, applications should be able to run anywhere regardless of compute. Running in
  [Lambda](https://aws.amazon.com/lambda/) should be no more difficult than running on
  [EC2](https://aws.amazon.com/ec2/)

- **Standards-based**: Based on [AsyncAPI](https://www.asyncapi.com/), and [JSON Schema](https://json-schema.org/). The
  framework should allow for easy documentation generation

- **Clean Implementation**: Each protocol should be implemented well; this means easy to use, and as optimal as possible

## Documentation

- For documentation on the [AMGI specification](https://amgi.readthedocs.io/en/latest/)
- For documentation on [AsyncFast](https://asyncfast.readthedocs.io/en/latest/)

## Servers

At the moment there are base implementations for the following protocols, these are all working
[AMGI](https://amgi.readthedocs.io/en/latest/) servers:

**Kafka:** [amgi-aiokafka](https://pypi.org/project/amgi-aiokafka/) is a basic [Kafka](https://kafka.apache.org/) sever
implementation

**MQTT:** [amgi-paho-mqtt](https://pypi.org/project/amgi-paho-mqtt/) is a basic [MQTT](https://mqtt.org/) sever
implementation

**Redis:** [amgi-redis](https://pypi.org/project/amgi-redis/) is a basic [Redis](https://redis.io/) server
implementation, soon with support for [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)

**SQS:** [amgi-aiobotocore](https://pypi.org/project/amgi-aiobotocore/) contains a basic
[SQS](https://aws.amazon.com/sqs/) sever implementation.
[amgi-sqs-event-source-mapping](https://pypi.org/project/amgi-sqs-event-source-mapping/) allows you to run an
application in [Lambda](https://aws.amazon.com/lambda/), with it translating the SQS Lambda event to the necessary
[AMGI](https://amgi.readthedocs.io/en/latest/) calls

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).
