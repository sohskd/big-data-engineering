package com.big.data.engineering3.producer;

import com.big.data.engineering3.annotations.EnablePublish;
import com.big.data.engineering3.dto.VLEDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;

import java.util.function.Consumer;

import static com.big.data.engineering3.constants.KafkaConstants.TOPIC;

@Slf4j
@EnablePublish
public class TestProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final VLEDto vleDto;

    public TestProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, VLEDto vleDto) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.vleDto = vleDto;
    }

    @PostConstruct
    public void sendMessage() {

        Consumer<Object> sendToKafka = data -> {
            try {
                val strData = this.objectMapper.writeValueAsString(data);
                log.info(String.format("Published Message -> Topic: %s, message: -> %s", TOPIC, strData));
                kafkaTemplate.send(TOPIC, strData);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };

        this.vleDto.getStudentAssessmentList().forEach(sendToKafka);
        this.vleDto.getAssessmentList().forEach(sendToKafka);
    }
}
