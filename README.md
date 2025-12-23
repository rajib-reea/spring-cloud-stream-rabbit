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
## Loss Prevention / Duplicate Avoidance(not implemented)

We need Outbox on the Producer side and Inbox on the Consumer side.
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

## ISO-8583 Flow Summary(not implemented)

````
ISO-8583 Request
      ↓
RabbitMQ
      ↓
Core Banking Consumer
      ↓
┌──────────────┬──────────────────┐
│ ISO Code     │ Action           │
├──────────────┼──────────────────┤
│ 00           │ ACK              │
│ 05 / 51 / 55 │ DLQ              │
│ 91 / 96      │ Retry → DLQ      │
└──────────────┴──────────────────┘

✅ SUCCESS

| ISO Code | Meaning  | Category | RabbitMQ Action | Retry |
| -------- | -------- | -------- | --------------- | ----- |
| **00**   | Approved | SUCCESS  | ACK             | ❌ No  |

❌ CLIENT / VALIDATION ERRORS (NON-RETRYABLE)

These indicate bad request / customer / card data
Never retry – send directly to DLQ

| ISO Code | Meaning                       | Category   | RabbitMQ Action | Retry |
| -------- | ----------------------------- | ---------- | --------------- | ----- |
| **05**   | Do not honor                  | Business   | DLQ             | ❌ No  |
| **12**   | Invalid transaction           | Validation | DLQ             | ❌ No  |
| **13**   | Invalid amount                | Validation | DLQ             | ❌ No  |
| **14**   | Invalid card number           | Validation | DLQ             | ❌ No  |
| **30**   | Format error                  | Validation | DLQ             | ❌ No  |
| **41**   | Lost card                     | Security   | DLQ             | ❌ No  |
| **43**   | Stolen card                   | Security   | DLQ             | ❌ No  |
| **54**   | Expired card                  | Validation | DLQ             | ❌ No  |
| **55**   | Incorrect PIN                 | Security   | DLQ             | ❌ No  |
| **57**   | Transaction not permitted     | Business   | DLQ             | ❌ No  |
| **58**   | Txn not permitted on terminal | Business   | DLQ             | ❌ No  |

💰 FINANCIAL / BUSINESS RULE ERRORS (NON-RETRYABLE)

Transaction is valid, but business rules reject it
Never retry

| ISO Code | Meaning                  | Category  | RabbitMQ Action | Retry |
| -------- | ------------------------ | --------- | --------------- | ----- |
| **51**   | Insufficient funds       | Financial | DLQ             | ❌ No  |
| **61**   | Exceeds withdrawal limit | Financial | DLQ             | ❌ No  |
| **62**   | Restricted card          | Financial | DLQ             | ❌ No  |
| **65**   | Exceeds frequency limit  | Financial | DLQ             | ❌ No  |

🔒 SECURITY / FRAUD ERRORS (NON-RETRYABLE)

Hard stops, security violations

| ISO Code | Meaning            | Category | RabbitMQ Action | Retry |
| -------- | ------------------ | -------- | --------------- | ----- |
| **59**   | Suspected fraud    | Security | DLQ             | ❌ No  |
| **63**   | Security violation | Security | DLQ             | ❌ No  |
| **75**   | PIN tries exceeded | Security | DLQ             | ❌ No  |

⚠️ DEFAULT / FALLBACK

| ISO Code | Meaning            | Category | RabbitMQ Action | Retry |
| -------- | ------------------ | -------- | --------------- | ----- |
| **96**   | System malfunction | System   | Retry → DLQ     | ✅ Yes |

Map<String, HandlingPolicy> ISO_ERROR_POLICY = Map.ofEntries(
    // Success
    entry("00", HandlingPolicy.ACK),

    // Non-retryable
    entry("05", HandlingPolicy.DLQ),
    entry("12", HandlingPolicy.DLQ),
    entry("13", HandlingPolicy.DLQ),
    entry("14", HandlingPolicy.DLQ),
    entry("30", HandlingPolicy.DLQ),
    entry("41", HandlingPolicy.DLQ),
    entry("43", HandlingPolicy.DLQ),
    entry("51", HandlingPolicy.DLQ),
    entry("54", HandlingPolicy.DLQ),
    entry("55", HandlingPolicy.DLQ),
    entry("57", HandlingPolicy.DLQ),
    entry("58", HandlingPolicy.DLQ),
    entry("61", HandlingPolicy.DLQ),
    entry("62", HandlingPolicy.DLQ),
    entry("65", HandlingPolicy.DLQ),
    entry("59", HandlingPolicy.DLQ),
    entry("63", HandlingPolicy.DLQ),
    entry("75", HandlingPolicy.DLQ),

    // Retryable
    entry("68", HandlingPolicy.RETRY),
    entry("91", HandlingPolicy.RETRY),
    entry("92", HandlingPolicy.RETRY),
    entry("94", HandlingPolicy.RETRY),
    entry("96", HandlingPolicy.RETRY_THEN_DLQ)
);


````