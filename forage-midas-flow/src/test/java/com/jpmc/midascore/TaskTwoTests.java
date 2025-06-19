package com.jpmc.midascore;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"transactions"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    /*@Autowired
    private TransactionListener transactionListener;
    */
    @BeforeAll
    void createTopic() throws ExecutionException, InterruptedException {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString()
        );

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(
                    Collections.singletonList(new NewTopic("transactions", 1, (short) 1))
            ).all().get();} catch (ExecutionException e) {
    if (e.getCause() instanceof TopicExistsException) {
        System.out.println("Topic already exists, skipping creation.");
    } else {
        throw e;
    }
}
Thread.sleep(3000);
        }

        // Optional: Wait to ensure metadata is updated
        //Thread.sleep(2000);
    
    
    @Test
    void task_two_verifier() throws InterruptedException {
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to watch for incoming transactions");
        logger.info("kill this test once you find the answer");
        while (true) {
            Thread.sleep(20000);
            logger.info("...");
        }
    }

}
