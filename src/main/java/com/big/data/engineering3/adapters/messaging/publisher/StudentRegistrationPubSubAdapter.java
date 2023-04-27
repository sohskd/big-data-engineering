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

import static com.big.data.engineering3.constant.GoogleCloudConstants.STUDENT_REGISTRATION_TOPIC;

@Slf4j
@Service
public class StudentRegistrationPubSubAdapter implements PubSubPortOut {

    private static final String STUDENT_REGISTRATION_CHANNEL_OUT = "StudentRegistrationChannelOut";

    private StudentRegistrationPubsubOutboundGateway studentRegistrationMessagingGateway;

    @Autowired
    public StudentRegistrationPubSubAdapter(StudentRegistrationPubsubOutboundGateway studentRegistrationMessagingGateway) {
        this.studentRegistrationMessagingGateway = studentRegistrationMessagingGateway;
    }

    @Override
    public void publish(String data) {
        log.info(String.format("Published Message -> Topic: %s, message: -> %s", STUDENT_REGISTRATION_TOPIC, data));
        studentRegistrationMessagingGateway.sendToPubsub(data);
    }

    @MessagingGateway(defaultRequestChannel = STUDENT_REGISTRATION_CHANNEL_OUT)
    public interface StudentRegistrationPubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    /**
     * An outbound channel adapter listens to new messages from a Spring channel and publishes them to a
     * Google Cloud Pub/Sub topic.
     */

    @Bean
    @ServiceActivator(inputChannel = STUDENT_REGISTRATION_CHANNEL_OUT)
    public MessageHandler studentRegistrationMessageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, STUDENT_REGISTRATION_TOPIC);
    }
}