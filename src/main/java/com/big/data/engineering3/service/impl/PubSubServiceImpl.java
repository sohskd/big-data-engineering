package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.PubSubAdapter;
import com.big.data.engineering3.domain.Assessment;
import com.big.data.engineering3.domain.StudentAssessment;
import com.big.data.engineering3.service.PubSubService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
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
public class PubSubServiceImpl implements PubSubService {

    private static final String TOPIC = "projects/ebd2023/topics/test-topic";

    private static final String DES_RAW_CSV_ASSESSMENTS = "des_raw_csv/assessments.csv";
    private static final String DOWNLOADED_DES_RAW_CSV_ASSESSMENTS = "data/downloaded/des_raw_csv/assessments.csv";
    private static final String DES_RAW_CSV_STUDENT_ASSESSMENT = "des_raw_csv/studentAssessment.csv";
    private static final String DOWNLOADED_DES_RAW_CSV_STUDENT_ASSESSMENT = "data/downloaded/des_raw_csv/studentAssessment.csv";

    private final PubSubAdapter pubSubAdapter;
    private final ObjectMapper objectMapper;

    @Autowired
    public PubSubServiceImpl(PubSubAdapter pubSubAdapter, ObjectMapper objectMapper) {
        this.pubSubAdapter = pubSubAdapter;
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

    private void publishVLEData(String fileLocation) throws FileNotFoundException {

        Consumer<Object> sendToPubSub = data -> {
            try {
                val strData = this.objectMapper.writeValueAsString(data);
                log.info(String.format("Published Message -> Topic: %s, message: -> %s", TOPIC, strData));
                pubSubAdapter.publish(strData);
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
        return new CsvToBeanBuilder(new FileReader(data)).withSkipLines(1).withType(clazz).build().parse();
    }
}
