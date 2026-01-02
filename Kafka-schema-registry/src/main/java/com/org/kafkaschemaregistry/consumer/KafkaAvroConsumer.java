package com.org.kafkaschemaregistry.consumer;

import com.org.kafkaschemaregistry.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaAvroConsumer {

    @KafkaListener(topics = "registry-topic")
    public void readMessages(ConsumerRecord<String, Employee> consumerRecord){
        String key = consumerRecord.key();
        Employee value = consumerRecord.value();
        log.info("Avro message received for key : {} and value : {}", key, value);
    }
}
