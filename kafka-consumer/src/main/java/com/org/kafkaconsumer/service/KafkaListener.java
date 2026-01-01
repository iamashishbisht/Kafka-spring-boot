package com.org.kafkaconsumer.service;

import com.org.common.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic2", groupId = "consumer-group1")
    public void consume(String message) {
        log.info("consumer consume the message {}", message);
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic2", groupId = "consumer-group1")
    public void consume1(String message) {
        log.info("consumer1 consume the message {}", message);
    }

    /*@org.springframework.kafka.annotation.KafkaListener(topics = "my-topic6", groupId = "consumer-group1")
    public void consume3(Customer customer) {
        log.info("consumer3 consumed the message {}", customer);
    }*/

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic6", groupId = "consumer-group1",
            topicPartitions = {@TopicPartition(
                    topic = "my-topic6",
                    partitions = {"2"}
            )})
    public void consumeMessageFromSpecificPartition(Customer customer) {
        log.info("consumeMessageFromSpecificPartition consumed the message {}", customer);
    }
}
