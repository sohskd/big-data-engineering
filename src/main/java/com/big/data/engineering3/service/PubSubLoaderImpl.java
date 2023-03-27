package com.big.data.engineering3.service;

import com.big.data.engineering3.domain.Assessment;
import com.big.data.engineering3.domain.StudentAssessment;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import com.google.cloud.storage.Blob;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reference: https://spring.io/guides/gs/messaging-gcp-pubsub/
 */
@Slf4j
@Component
public class PubSubLoaderImpl implements PubSubLoader {

    private static final String TOPIC = "projects/ebd2023/topics/test-topic";

    private static final String SUBSCRIPTION_NAME = "projects/ebd2023/subscriptions/test-topic-sub";

    private static final String DES_RAW_CSV_ASSESSMENTS = "des_raw_csv/assessments.csv";
    private static final String DOWNLOADED_DES_RAW_CSV_ASSESSMENTS = "data/downloaded/des_raw_csv/assessments.csv";
    private static final String DES_RAW_CSV_STUDENT_ASSESSMENT = "des_raw_csv/studentAssessment.csv";
    private static final String DOWNLOADED_DES_RAW_CSV_STUDENT_ASSESSMENT = "data/downloaded/des_raw_csv/studentAssessment.csv";

    private PubsubOutboundGateway messagingGateway;
    private ObjectMapper objectMapper;

    @Autowired
    public PubSubLoaderImpl(PubsubOutboundGateway messagingGateway, ObjectMapper objectMapper) {
        this.messagingGateway = messagingGateway;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publishData(List<Blob> blobList) {

        blobList.forEach(b -> {
            String fileLocation = b.getName();
            try {
                publishVLEData(fileLocation);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * An outbound channel adapter listens to new messages from a Spring channel and publishes them to a
     * Google Cloud Pub/Sub topic.
     */

    @Bean
    @ServiceActivator(inputChannel = "pubsubOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, TOPIC);
    }


    @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
    public interface PubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    /**
     * Create an inbound channel adapter. An inbound channel adapter listens to messages from a Google Cloud Pub/Sub
     * subscription and sends them to a Spring channel in an application.
     */

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier("pubsubInputChannel") MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, SUBSCRIPTION_NAME);
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    public MessageChannel pubsubInputChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "pubsubInputChannel")
    public MessageHandler messageReceiver() {
        return message -> {
            log.info("Message arrived! Payload: " + new String((byte[]) message.getPayload()));
            BasicAcknowledgeablePubsubMessage originalMessage =
                    message.getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
            originalMessage.ack();
        };
    }

    private void publishVLEData(String fileLocation) throws FileNotFoundException {

        Consumer<Object> sendToPubSub = data -> {
            try {
                val strData = this.objectMapper.writeValueAsString(data);
                log.info(String.format("Published Message -> Topic: %s, message: -> %s", TOPIC, strData));
                messagingGateway.sendToPubsub(strData);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };

        switch (fileLocation) {
            case DES_RAW_CSV_ASSESSMENTS ->
                    readFile(DOWNLOADED_DES_RAW_CSV_ASSESSMENTS, Assessment.class).forEach(sendToPubSub);
            case DES_RAW_CSV_STUDENT_ASSESSMENT ->
                    readFile(DOWNLOADED_DES_RAW_CSV_STUDENT_ASSESSMENT, StudentAssessment.class).forEach(sendToPubSub);
            default -> log.info("Nothing");
        }
    }

    private <T> List<T> readFile(String data, Class<T> clazz) throws FileNotFoundException {
        log.info(data);
        return new CsvToBeanBuilder(new FileReader(data))
                .withSkipLines(1)
                .withType(clazz)
                .build()
                .parse();
    }
}
