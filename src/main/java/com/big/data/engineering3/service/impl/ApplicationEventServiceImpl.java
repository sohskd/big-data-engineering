package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.events.spring.TriggerEvent;
import com.big.data.engineering3.service.ApplicationEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ApplicationEventServiceImpl implements ApplicationEventService {

    private final ApplicationEventPublisher applicationEventPublisher;
    private final ObjectMapper objectMapper;

    @Autowired
    public ApplicationEventServiceImpl(ApplicationEventPublisher applicationEventPublisher, ObjectMapper objectMapper) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String payload, Message message) {
        BasicAcknowledgeablePubsubMessage originalMessage =
                message.getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
        assert originalMessage != null;
        originalMessage.ack();
        try {
            val m = objectMapper.readValue(payload, Map.class);
            applicationEventPublisher.publishEvent(new TriggerEvent(this,
                    (String) m.get("name"), (String) m.get("bucket")
            ));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
