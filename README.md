# spring-cloud-stream-rabbit
## Run RabbitMQ
```
use run-instruction.txt for running rabbitmq cluster.
```
## Metrics
Message sent count is available at the following endpoints
```bash
http://localhost:8080/actuator/prometheus
http://localhost:8080/actuator/metrics/messages.sent.direct
```