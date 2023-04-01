package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.messaging.publisher.PubSubAdapter;
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
import java.util.Map;
import java.util.function.Consumer;

/**
 * Reference: https://spring.io/guides/gs/messaging-gcp-pubsub/
 */
@Slf4j
@Component
public class PubSubServiceImpl implements PubSubService {

    private static final String TOPIC = "projects/ebd2023/topics/test-topic";

    private final PubSubAdapter pubSubAdapter;
    private final ObjectMapper objectMapper;
    private final Map<String, Map<String, String>> googleCloudFileConfigs;

    @Autowired
    public PubSubServiceImpl(PubSubAdapter pubSubAdapter, ObjectMapper objectMapper, Map<String,
            Map<String, String>> googleCloudFileConfigs) {
        this.pubSubAdapter = pubSubAdapter;
        this.objectMapper = objectMapper;
        this.googleCloudFileConfigs = googleCloudFileConfigs;
    }

    @Override
    public void publishVLEData(String fileLocation) throws FileNotFoundException, ClassNotFoundException {

        Consumer<Object> sendToPubSub = data -> {
            try {
                val strData = this.objectMapper.writeValueAsString(data);
                log.info(String.format("Published Message -> Topic: %s, message: -> %s", TOPIC, strData));
                pubSubAdapter.publish(strData);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };

        // fileLocation = des_raw_csv/1_assessments.csv
        // fileLocation = des_raw_csv/assessments_20230401.csv
        String detectFile = fileLocation.substring(fileLocation.lastIndexOf('/') + 1);
        Map<String, String> downloadedFileAndClassInfo = googleCloudFileConfigs.get(detectFile.substring(0, 1));
        String fileDownloadedPath = downloadedFileAndClassInfo.get("downloadedFileName")
                + fileLocation.substring(fileLocation.lastIndexOf('/') + 1);
        readFile(fileDownloadedPath, Class.forName(downloadedFileAndClassInfo.get("className"))).forEach(sendToPubSub);
    }

    private <T> List<T> readFile(String data, Class<T> clazz) throws FileNotFoundException {
        log.info(data);
        return new CsvToBeanBuilder(new FileReader(data)).withSkipLines(1).withType(clazz).build().parse();
    }
}
