package com.big.data.engineering3.adapters.messaging.subscriber.google;

import com.big.data.engineering3.ports.portin.PubSubPortIn;
import com.big.data.engineering3.service.ApplicationEventService;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Service;

import static com.big.data.engineering3.constant.GoogleCloudConstants.RAW_SUBSCRIPTION_NAME;

@Slf4j
@Service
public class RawZoneSubAdapter implements PubSubPortIn {

    private static final String INPUT_CHANNEL = "rawZoneSpringInputChannel";

    private final ApplicationEventService applicationEventService;

    @Autowired
    public RawZoneSubAdapter(ApplicationEventService applicationEventService) {
        this.applicationEventService = applicationEventService;
    }

    @Bean
    @ServiceActivator(inputChannel = INPUT_CHANNEL)
    public MessageHandler rawZoneReceiver() {
        return message -> {
            String payload = new String((byte[]) message.getPayload());
            log.info("[Raw Zone Changed] Message arrived! Payload: " + payload);
            this.applicationEventService.publish(payload, message);
        };
    }

    /**
     * Create an inbound channel adapter. An inbound channel adapter listens to messages from a Google Cloud Pub/Sub
     * subscription and sends them to a Spring channel in an application.
     */

    @Bean
    public PubSubInboundChannelAdapter rawZoneChannelAdapter(
            @Qualifier(INPUT_CHANNEL) MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, RAW_SUBSCRIPTION_NAME);
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    public MessageChannel rawZoneSpringInputChannel() {
        return new DirectChannel();
    }
}
