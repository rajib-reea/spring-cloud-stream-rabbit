# Spring Cloud Stream – RabbitMQ

This project demonstrates the use of **Spring Cloud Stream** with **RabbitMQ** using the **functional programming model** (`Supplier` / `Consumer`).

The application produces and consumes messages without using any broker-specific APIs, keeping the code clean, decoupled, and transport-agnostic.

---

## Overview

The application defines:

- A **message producer** using `Supplier<String>`
- A **message consumer** using `Consumer<String>`
- A **global error handler** using `Consumer<ErrorMessage>`
- Metrics using **Micrometer**

All messaging infrastructure (exchanges, queues, bindings) is automatically managed by **Spring Cloud Stream**.

---

## Why This Is Spring Cloud Stream

Even though the code does not contain any explicit Spring Cloud Stream annotations, the application **does use Spring Cloud Stream**.

When the following dependencies are present:

- `spring-cloud-stream`
- `spring-cloud-stream-binder-rabbit`

Spring Cloud Stream automatically:

- Detects functional beans (`Supplier`, `Consumer`)
- Creates input and output bindings (`source-out-0`, `sink-in-0`)
- Connects those bindings to RabbitMQ
- Handles message conversion, routing, and error channels

Without Spring Cloud Stream on the classpath, these beans would behave like normal Spring beans and **no messaging would occur**.

---

## Messaging Flow

At runtime, the message flow is:

Supplier<String> → source-out-0 → RabbitMQ Exchange
RabbitMQ Queue → sink-in-0 → Consumer<String>

The names of exchanges and queues are auto-generated unless explicitly configured.

---

## Error Handling

The application defines a custom error handler:

```java
@Bean
public Consumer<ErrorMessage> myErrorHandler() {
    return error -> log.error("App in error {}", error.getOriginalMessage());
}
```
Spring Cloud Stream automatically routes message-processing errors to this handler using its internal error channels.
## Run RabbitMQ
```
use run-instruction.txt infra-code/rabbit-mq for running rabbitmq cluster.
```
## Metrics
Message sent count is available at the following endpoints
```bash
http://localhost:8080/actuator/prometheus
http://localhost:8080/actuator/metrics/messages.sent.direct
```

````
Example Payload
{
  "txId": "TX123",
  "customerId": "C456",
  "type": "SMS",
  "message": "BDT 5000 credited",
  "timestamp": "2025-01-22T10:30:00Z"
}

````

We need Outbox on the Producer side and Inbox on the Consumer side(not implemented).
````
1. Outbox:

CREATE TABLE outbox (
  id UUID PRIMARY KEY,
  aggregate_type VARCHAR(50),
  aggregate_id VARCHAR(50),
  event_type VARCHAR(50),
  payload JSONB,
  status VARCHAR(20), -- NEW, SENT, FAILED
  created_at TIMESTAMP
);

@Transactional
public void handle(Message msg) {
    updateAccount();
    saveOutboxEvent(msg);
}

@Scheduled(fixedDelay = 1000)
public void publishOutbox() {
    List<OutboxEvent> events = repo.findUnsent();
    for (var e : events) {
        rabbitTemplate.convertAndSend(...);
        e.markSent();
    }
}

2. Inbox:

CREATE TABLE inbox (
  message_id VARCHAR(100) PRIMARY KEY,
  received_at TIMESTAMP
);

@Transactional
public void consume(Message<?> message) {

    String messageId = message.getHeaders()
                              .getId()
                              .toString();

    if (inboxRepository.existsById(messageId)) {
        // Already processed → safe to ACK
        return;
    }

    // 1️⃣ Business logic
    processBusiness(message.getPayload());

    // 2️⃣ Mark as processed
    inboxRepository.save(new InboxMessage(messageId));
}

````
❌ Skip Outbox if:

You don’t care if message is lost

You produce events after commit manually

You accept inconsistency

❌ Skip Inbox if:

Consumer logic is read-only

Side effects are idempotent by nature

Duplicates are harmless