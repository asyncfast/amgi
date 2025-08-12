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
