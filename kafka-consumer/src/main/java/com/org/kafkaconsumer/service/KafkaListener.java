package com.org.kafkaconsumer.service;

import com.org.common.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic2", groupId = "consumer-group1")
    public void consume(String message) {
        log.info("consumer consumed the message {}", message);
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic2", groupId = "consumer-group1")
    public void consume1(String message) {
        log.info("consumer1 consumed the message {}", message);
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic6", groupId = "consumer-group1")
    public void consume2(Customer customer) {
        log.info("consumer2 consumed the message {}", customer);
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic7", groupId = "consumer-group1",
            topicPartitions = {@TopicPartition(
                    topic = "my-topic7",
                    partitions = {"2"}
            )})
    public void consumeMessageFromSpecificPartition(Customer customer) {
        log.info("consumeMessageFromSpecificPartition consumed the message {}", customer);
    }

    //attempts 4 means 3 times will be retried, N-1
//    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 2, maxDelay = 1000)
//            , exclude = NullPointerException.class)
    @RetryableTopic(attempts = "4")
    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic10", groupId = "consumer-group1")
    //added headers to get info and logging into console, even it is not required if not using anywhere
    public void consumerErrorHandling(Customer customer,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("consumerErrorHandling consumed the message {}, topic {}, offset {} ", customer, topic, offset);
        //simulating external APIs/db operations which failed
        throw new RuntimeException("Failed to save or store data");
    }

    @DltHandler
    public void listenDLT(Customer customer,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.OFFSET) long offset){
        log.info("dlt handler consumed the message {}, topic {}, offset {} ", customer, topic, offset);
    }


}
