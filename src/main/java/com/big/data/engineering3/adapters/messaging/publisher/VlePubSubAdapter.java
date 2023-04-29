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

import static com.big.data.engineering3.constant.GoogleCloudConstants.VLE_TOPIC;

@Slf4j
@Service
public class VlePubSubAdapter implements PubSubPortOut {

    private static final String VLE_CHANNEL_OUT = "VleChannelOut";

    private VlePubsubOutboundGateway vleMessagingGateway;

    @Autowired
    public VlePubSubAdapter(VlePubsubOutboundGateway VleMessagingGateway) {
        this.vleMessagingGateway = VleMessagingGateway;
    }

    @Override
    public void publish(String data) {
        log.info(String.format("Published Message -> Topic: %s, message: -> %s", VLE_TOPIC, data));
        vleMessagingGateway.sendToPubsub(data);
    }

    @MessagingGateway(defaultRequestChannel = VLE_CHANNEL_OUT)
    public interface VlePubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    /**
     * An outbound channel adapter listens to new messages from a Spring channel and publishes them to a
     * Google Cloud Pub/Sub topic.
     */

    @Bean
    @ServiceActivator(inputChannel = VLE_CHANNEL_OUT)
    public MessageHandler vleMessageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, VLE_TOPIC);
    }
}