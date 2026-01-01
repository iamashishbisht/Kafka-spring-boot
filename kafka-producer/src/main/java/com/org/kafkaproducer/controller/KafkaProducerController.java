package com.org.kafkaproducer.controller;

import com.org.common.Customer;
import com.org.kafkaproducer.service.KafkaMessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/kafka-producer")
public class KafkaProducerController {

    private final KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            kafkaMessagePublisher.sendMessageToTopic(message);
            /*for (int i = 0; i <= 10000; i++) {
                kafkaMessagePublisher.sendMessageToTopic(String.valueOf(i));
            }*/
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish")
    public ResponseEntity<?> publishMessage(@RequestBody Customer customer) {
        try {
            kafkaMessagePublisher.sendMessageToTopic(customer);
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            log.info("exception is :"+e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish/partition")
    public ResponseEntity<?> publishMessageToSpecificTopicPartition(@RequestBody Customer customer) {
        try {
            kafkaMessagePublisher.sendMessageToSpecificTopicPartition(customer);
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            log.info("exception is :"+e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
