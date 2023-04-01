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

import static com.big.data.engineering3.constant.GoogleCloudConstants.CHANNEL_OUT;
import static com.big.data.engineering3.constant.GoogleCloudConstants.TOPIC;

@Slf4j
@Service
public class PubSubAdapter implements PubSubPortOut {

    private PubsubOutboundGateway messagingGateway;


    @Autowired
    public PubSubAdapter(PubsubOutboundGateway messagingGateway) {
        this.messagingGateway = messagingGateway;
    }

    @Override
    public void publish(String data) {
        messagingGateway.sendToPubsub(data);
    }

    /**
     * An outbound channel adapter listens to new messages from a Spring channel and publishes them to a
     * Google Cloud Pub/Sub topic.
     */

    @Bean
    @ServiceActivator(inputChannel = CHANNEL_OUT)
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, TOPIC);
    }

    @MessagingGateway(defaultRequestChannel = CHANNEL_OUT)
    public interface PubsubOutboundGateway {
        void sendToPubsub(String text);
    }
}
