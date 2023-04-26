package com.big.data.engineering3.adapters.messaging.publisher;

import com.big.data.engineering3.ports.portout.PubSubPortOut;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Service;

import static com.big.data.engineering3.constant.GoogleCloudConstants.STUDENT_VLE_TOPIC;

@Slf4j
@Service
public class StudentVlePubSubAdapter implements PubSubPortOut {

    private static final String STUDENT_VLE_CHANNEL_OUT = "StudentVleChannelOut";

    private StudentVlePubsubOutboundGateway studentVleMessagingGateway;

    @Autowired
    public StudentVlePubSubAdapter(StudentVlePubsubOutboundGateway studentVleMessagingGateway) {
        this.studentVleMessagingGateway = studentVleMessagingGateway;
    }

    @Override
    public void publish(String data) {
        log.info(String.format("Published Message -> Topic: %s, message: -> %s", STUDENT_VLE_TOPIC, data));
        studentVleMessagingGateway.sendToPubsub(data);
    }

    @MessagingGateway(defaultRequestChannel = STUDENT_VLE_CHANNEL_OUT)
    public interface StudentVlePubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    /**
     * An outbound channel adapter listens to new messages from a Spring channel and publishes them to a
     * Google Cloud Pub/Sub topic.
     */

    @Bean
    @ServiceActivator(inputChannel = STUDENT_VLE_CHANNEL_OUT)
    public MessageHandler studentVleMessageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, STUDENT_VLE_TOPIC);
    }
}