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

import static com.big.data.engineering3.constant.GoogleCloudConstants.LANDING_SUBSCRIPTION_NAME;

@Slf4j
@Service
public class LandingZoneSubAdapter implements PubSubPortIn {

    private static final String INPUT_CHANNEL = "landingZoneSpringInputChannel";

    private final ApplicationEventService applicationEventService;

    @Autowired
    public LandingZoneSubAdapter(ApplicationEventService applicationEventService) {
        this.applicationEventService = applicationEventService;
    }

    @Bean
    @ServiceActivator(inputChannel = INPUT_CHANNEL)
    public MessageHandler landingZoneReceiver() {
        return message -> {
            String payload = new String((byte[]) message.getPayload());
            log.info("[Landing Zone Changed] Message arrived! Payload: " + payload);
            this.applicationEventService.publish(payload, message);
        };
    }

    /**
     * Create an inbound channel adapter. An inbound channel adapter listens to messages from a Google Cloud Pub/Sub
     * subscription and sends them to a Spring channel in an application.
     */

    @Bean
    public PubSubInboundChannelAdapter landingZoneChannelAdapter(
            @Qualifier(INPUT_CHANNEL) MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, LANDING_SUBSCRIPTION_NAME);
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    public MessageChannel landingZoneSpringInputChannel() {
        return new DirectChannel();
    }
}
