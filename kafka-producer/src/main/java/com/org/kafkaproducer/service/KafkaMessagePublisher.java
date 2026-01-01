package com.org.kafkaproducer.service;

import com.org.common.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaMessagePublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessageToTopic(String message) {
        log.info("send message to topic");
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("my-topic2", message);
//        synchronized
//        completableFuture.get();
        completableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Message =[" + message + "] with offset=[" +
                        result.getRecordMetadata().offset() + "] with partition=[" +
                        result.getRecordMetadata().partition() + "]");
            } else {
                log.info("Unable to send message =[" + message + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendMessageToTopic(Customer customer) {
        log.info("send customer details to topic {}", customer);
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("my-topic6", customer);
        completableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Message =[" + customer + "] with offset=[" +
                        result.getRecordMetadata().offset() + "] with partition=[" +
                        result.getRecordMetadata().partition() + "]");
            } else {
                log.info("Unable to send message =[" + customer + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendMessageToSpecificTopicPartition(Customer customer) {
        log.info("send customer details to topic {}", customer);
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("my-topic6", 2,null, customer);
        completableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Message =[" + customer + "] with offset=[" +
                        result.getRecordMetadata().offset() + "] with partition=[" +
                        result.getRecordMetadata().partition() + "]");
            } else {
                log.info("Unable to send message =[" + customer + "] due to : " + ex.getMessage());
            }
        });
    }
}
