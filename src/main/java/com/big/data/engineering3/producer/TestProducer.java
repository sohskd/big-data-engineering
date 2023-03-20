package com.big.data.engineering3.producer;

import com.big.data.engineering3.annotations.EnablePublish;
import com.big.data.engineering3.domain.VLEDataDomain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;

import static com.big.data.engineering3.constants.KafkaConstants.TOPIC;

@Slf4j
@EnablePublish
public class TestProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final VLEDataDomain vleDataDomain;

    public TestProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, VLEDataDomain vleDataDomain) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.vleDataDomain = vleDataDomain;
    }

    @PostConstruct
    public void sendMessage() {

        this.vleDataDomain.getStudentAssessmentList().forEach(data -> {
                    try {
                        kafkaTemplate.send(TOPIC, this.objectMapper.writeValueAsString(data));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }
}
