package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionConsumer {

    @KafkaListener(
  topics = "${general.kafka-topic}",
  groupId = "midas-core-group",
  properties = {
    "spring.json.value.default.type=com.jpmc.midascore.foundation.Transaction"
  }
)
    public void listen(Transaction transaction) {
        System.out.println("✅ Received Transaction: " + transaction);
    }
}
