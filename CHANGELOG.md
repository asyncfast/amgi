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
