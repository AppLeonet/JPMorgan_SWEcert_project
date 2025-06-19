package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, Transaction> record) {
        Transaction transaction = record.value();
        System.out.println("Received transaction: " + transaction);
        // In the future, you can store or process it here
    }
}
