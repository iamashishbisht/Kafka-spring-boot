package com.org.kafkaschemaregistry.controller;

import com.org.kafkaschemaregistry.dto.Employee;
import com.org.kafkaschemaregistry.producer.KafkaAvroProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class EventController {

    private final KafkaAvroProducer kafkaAvroProducer;

    @PostMapping("/publish")
    public String sendMessage(@RequestBody Employee employee){
        kafkaAvroProducer.send(employee);
        return "message published";
    }
}
