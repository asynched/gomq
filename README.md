# GOMQ

A simple messaging queue implementation using Go.

## Registration

### Producer

Producers register a topic sending a TCP packet with the payload below:

```log
PRODUCER TOPIC /user-registered
```

This operation might fail if the topic is already registered, if that happens, the output payload is:

```log
MESSAGE ERR_ALREADY_REGISTERED
```

### Consumer

Consumers register themselves passing in the message the desired topic to subscribe

```log
CONSUMER TOPIC /user-registered
```

This might fail if the topic doesn't exist, resulting in the payload:

```log
MESSAGE ERR_PRODUCER_UNAVAILABLE
```
