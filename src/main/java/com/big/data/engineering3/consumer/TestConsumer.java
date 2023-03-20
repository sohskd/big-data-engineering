package com.big.data.engineering3.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import static com.big.data.engineering3.constants.KafkaConstants.TOPIC;

@Slf4j
@Service
public class TestConsumer {

    @KafkaListener(topics = TOPIC, groupId = "test-consumer-group-id")
    public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {

        log.info(String.format("Consumed Message -> Partition Id: %s, Offset Id: %s, message: -> %s",
                consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value()));
        ack.acknowledge();
    }
}
