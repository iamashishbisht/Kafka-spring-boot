package com.org.kafkaschemaregistry.producer;

import com.org.kafkaschemaregistry.dto.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaAvroProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(Employee employee){
        log.info("send employee details to topic {}", employee);
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send("registry-topic", UUID.randomUUID().toString(), employee);
        completableFuture.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Message =[" + employee + "] with offset=[" +
                        result.getRecordMetadata().offset() + "] with partition=[" +
                        result.getRecordMetadata().partition() + "]");
            } else {
                log.info("Unable to send message =[" + employee + "] due to : " + ex.getMessage());
            }
        });
    }
}
