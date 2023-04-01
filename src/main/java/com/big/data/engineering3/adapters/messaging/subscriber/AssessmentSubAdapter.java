package com.big.data.engineering3.adapters.messaging.subscriber;

import com.big.data.engineering3.ports.portin.PubSubPortIn;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AssessmentSubAdapter implements PubSubPortIn {

    private static final String SUBSCRIPTION_NAME = "projects/ebd2023/subscriptions/assessment-topic-sub";
    private static final String INPUT_CHANNEL = "assessmentSpringInputChannel";

    @Bean
    @ServiceActivator(inputChannel = INPUT_CHANNEL)
    public MessageHandler assessmentMessageReceiver() {
        return message -> {
            log.info("[Assessment] Message arrived! Payload: " + new String((byte[]) message.getPayload()));
            BasicAcknowledgeablePubsubMessage originalMessage =
                    message.getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
            assert originalMessage != null;
            originalMessage.ack();
        };
    }

    /**
     * Create an inbound channel adapter. An inbound channel adapter listens to messages from a Google Cloud Pub/Sub
     * subscription and sends them to a Spring channel in an application.
     */

    @Bean
    public PubSubInboundChannelAdapter assessmentMessageChannelAdapter(
            @Qualifier(INPUT_CHANNEL) MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, SUBSCRIPTION_NAME);
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    public MessageChannel assessmentSpringInputChannel() {
        return new DirectChannel();
    }
}
