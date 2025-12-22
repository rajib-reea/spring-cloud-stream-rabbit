package com.rgmf.demoscsrabbit;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.support.ErrorMessage;

@SpringBootApplication
public class DemoScsRabbitApplication {

  private static final Logger log = LoggerFactory.getLogger(DemoScsRabbitApplication.class);

  @Autowired
  private MeterRegistry meterRegistry;

  private final AtomicInteger counter = new AtomicInteger();

  public static void main(String[] args) {
    SpringApplication.run(DemoScsRabbitApplication.class, args);
  }

  @Bean
  public Supplier<String> source() {
    return () -> {
      String message = "Message %s".formatted(counter.incrementAndGet());
      log.info("Sending -> {}", message);

      meterRegistry.counter("messages.sent.direct").increment();

      return message;
    };
  }

  @Bean
  public Consumer<String> sink() {
    return message -> log.info("Received -> {}", message);
  }

  @Bean
  public Consumer<ErrorMessage> myErrorHandler() {
    return error -> log.error("************ App in error {} ************", error.getOriginalMessage());
  }

}
