## 0.31.0 (2026-01-29)

### Feat

- **amgi-aiokafka**: add message send manager so it can be used by other servers, and send elsewhere
- **amgi-aiobotocore**: add message send manager so it can be used by other servers, and send elsewhere
- **amgi-redis**: add message send manager so it can be used by other servers, and send elsewhere

## 0.30.0 (2026-01-29)

### Feat

- **amgi-aiokafka**: add auto_offset_reset option to server and runners

## 0.29.1 (2026-01-28)

### Fix

- **amgi-sqs-event-source-mapping**: create explicit event loop for python 3.14 compatibility

### Perf

- **amgi-aiokafka**: process partition records concurrently

## 0.29.0 (2026-01-27)

### Feat

- **asyncfast**: replace channel definition asserts with invalid channel definition error

## 0.28.1 (2026-01-27)

### Perf

- **asyncfast**: avoid json.loads when validating payloads

## 0.28.0 (2026-01-26)

### Feat

- **asyncfast**: support header aliases and preserve header casing
- **asyncfast**: include headers schema for outgoing messages in the asyncapi generation

### Refactor

- **asyncfast**: make channel and headers classes protected

### Perf

- **asyncfast**: avoid json.loads round-trip in header serialization

## 0.27.2 (2026-01-26)

### Fix

- **amgi-sqs-event-source-mapping**: guard against add_signal_handler not implemented error on windows
- **asyncfast-cli**: use select to find entry points as get is deprecated

### Refactor

- **asyncfast-cli**: refactor cli so that it is testable

## 0.27.1 (2026-01-26)

### Fix

- **asyncfast**: use annotation args when creating the headers model so it worked across all versions of python and pydantic v2
- **asyncfast**: stop using \_type from type adapter

## 0.27.0 (2026-01-22)

### Feat

- **amgi-aiobotocore**: add the message.ack.out_of_order extension to message scope so messages can be handled out of order
- **amgi-sqs-event-source-mapping**: add the message.ack.out_of_order extension to message scope so messages can be handled out of order
- **asyncfast**: add support for the message.ack.out_of_order to allow for out of order handling
- **amgi-types**: add support for extensions to the amgi message scope

## 0.26.1 (2025-12-18)

### Fix

- **amgi-sqs-event-source-mapping**: close client on shutdown if the client has been instantiated

### Refactor

- **amgi-sqs-event-source-mapping**: make boto3 client a cached property so it is only instantiatied at call time

## 0.26.0 (2025-12-18)

### Feat

- **amgi-common**: run servers using a new event loop, this adds multiprocess support

### Fix

- **amgi-paho-mqtt**: fix server so it does not attempt to get event loop at initialisation

## 0.25.2 (2025-12-18)

### Fix

- **amgi-sqs-event-source-mapping**: make boto3 an optional dependency
- **amgi-types**: fix typing-extensions dependency
- **amgi-common**: fix dependencies and types

## 0.25.1 (2025-12-03)

### Fix

- **asyncfast**: allow for pipe unions for sending types

### Refactor

- update syntax to python3.10

## 0.25.0 (2025-12-02)

### Feat

- **amgi-common**: add operation cacher

### Fix

- **amgi-common**: stoppable will not start task if already cancelled

### Refactor

- **amgi-common**: update operation batcher so it groups items

### Perf

- **amgi-sqs-event-source-mapping**: use caching, and batching to improve sqs performance
- **amgi-aiobotocore**: use caching, and batching to improve sqs performance

## 0.24.1 (2025-12-01)

### Refactor

- **amgi-redis**: add default to url argument for run function

## 0.24.0 (2025-11-26)

### Feat

- **amgi-common**: move graceful serving code to amgi-common so it can be reused

### Fix

- **amgi-redis**: use server_serve from amgi-common so server is shutdown gracefully
- **amgi-paho-mqtt**: use server_serve from amgi-common so server is shutdown gracefully
- **amgi-aiobotocore**: use server_serve from amgi-common so server is shutdown gracefully
- **amgi-paho-mqtt**: handle publish correctly, waiting for success, or failure

### Refactor

- **amgi-redis**: rename send, and receive classes so they match the naming convention of other servers

## 0.23.1 (2025-11-26)

### Refactor

- **amgi-aiokafka**: split send and receive into two seperate classes

## 0.23.0 (2025-11-26)

### Feat

- **amgi-common**: allow state to be passed into Lifespan context manager

### Fix

- **amgi-sqs-event-source-mapping**: correctly handle amgi state after startup
- **amgi-paho-mqtt**: correctly handle amgi state after startup
- **asyncfast**: fix behaviour of lifespan, it is expected to be called with the running application
- **amgi-common**: correctly handle lifespan failures

## 0.22.0 (2025-11-26)

### Feat

- create new asyncfast-cli package to allow dependencies to be optional

## 0.21.0 (2025-11-20)

### Feat

- **asyncfast**: add support for lifespan
- **amgi-sqs-event-source-mapping**: add integrity check to records, only processing valid records
- **amgi-sqs-event-source-mapping**: add support for lifespan
- add sqs lambda support

## 0.20.0 (2025-11-18)

### Feat

- **amgi-redis**: add redis entrypoint and example
- add a redis amgi server

### Fix

- **amgi-aiobotocore**: fix amgi dependencies
- **amgi-paho-mqtt**: fix amgi dependencies

## 0.19.0 (2025-11-15)

### Feat

- **amgi-aiobotocore**: add sqs entrypoint and example
- add sqs support

## 0.18.0 (2025-11-13)

### Feat

- **asyncfast**: add a method to send messages without yielding

## 0.17.0 (2025-11-13)

### Feat

- **amgi-paho-mqtt**: keep track of handling tasks, only closing the client when all handling tasks are complete
- **amgi-paho-mqtt**: add entrypoint for amgi-paho-mqtt so that it can be run via the cli
- **amgi-paho-mqtt**: add the ability to send from the mqtt amgi server
- add new amgi server for mqtt

## 0.16.0 (2025-11-12)

### Feat

- remove python3.9 support
- **amgi-common**: add OperationBatcher to allow for simple batch operations

## 0.15.2 (2025-10-30)

### Fix

- **amgi-aiokafka**: make test-utils a development dependency

## 0.15.1 (2025-10-29)

### Refactor

- add new Stoppable class to amgi-common, this should allow for simple code to stop receive loops

## 0.15.0 (2025-10-19)

### Feat

- add state to scopes, allowing startup to store state

### Fix

- use fixed version of pydantic

## 0.14.0 (2025-09-28)

### Feat

- add support for bindings to the message class
- add initial support for bindings

### Refactor

- simplify binding by using protocol, and field name instead of path

## 0.13.1 (2025-09-16)

### Refactor

- share send message code

## 0.13.0 (2025-09-16)

### Feat

- add support for synchronous handlers

### Fix

- send messages yielded during an athrow while handling an asynchronous generator

## 0.12.2 (2025-09-14)

### Fix

- fix the message send annotation check for Message type

## 0.12.1 (2025-09-14)

### Fix

- any errors raised during a send are thrown in the handler

### Refactor

- upgrade to python3.9 features with pyupgrade

## 0.12.0 (2025-08-31)

### Feat

- add support for ack and nack in the amgi spec
- allow for graceful exit of amgi-aiokafka when exciting

## 0.11.0 (2025-08-30)

### Feat

- add initial support for typed message sending

## 0.10.0 (2025-08-16)

### Feat

- add run command to asyncfast cli

## 0.9.1 (2025-08-16)

### Fix

- remove schema from components if there are no definitions

## 0.9.0 (2025-08-13)

### Feat

- rename project to AMGI to better reflect how it can be used

## 0.8.0 (2025-08-12)

### Feat

- add initial support for address parameters
- add support for simple and nested payloads
- use FieldInfo for Header so attributes such as description can be used in the schema generation
- add support for dataclasses

### Fix

- fix common-acgi version

### Refactor

- remove \_generate_schemas as it is no longer used
- remove \_pascal_case and add title property to Channel
- use acgi TypedDicts to reduce IDE warnings

## 0.7.0 (2025-08-10)

### Feat

- introduce cli, allowing for generation of AsyncAPI
- add support for header alias

### Fix

- add full versions back so they can be detected by commitizen

### Refactor

- move lifespan to new project common-acgi

## 0.6.0 (2025-08-08)

### Fix

- use fixed version for all references to types-acgi

## 0.5.0 (2025-08-08)

### Feat

- initial implementation of aiokafka-acgi

### Refactor

- make payload optional in the message scope, remove subscriptions from lifespan.startup.complete

## 0.4.0 (2025-08-07)

### Feat

- initial work for sending to other channels

### Fix

- support all types of default arguments not just optional
- fix optional beader support

## 0.3.0 (2025-08-07)

### Feat

- add initial header support to AsyncFast
- return chancel function so it can be easily tested

## 0.2.0 (2025-08-05)

### Feat

- limit message scope to single message
