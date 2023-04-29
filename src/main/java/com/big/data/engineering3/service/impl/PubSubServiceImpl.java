package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.messaging.publisher.*;
import com.big.data.engineering3.domain.*;
import com.big.data.engineering3.service.PubSubService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

/**
 * Reference: https://spring.io/guides/gs/messaging-gcp-pubsub/
 */
@Slf4j
@Component
public class PubSubServiceImpl implements PubSubService {

    private final StudentAssessmentPubSubAdapter studentAssessmentPubSubAdapter;
    private final StudentInfoPubSubAdapter studentInfoPubSubAdapter;
    private final StudentRegistrationPubSubAdapter studentRegistrationPubSubAdapter;
    private final StudentVlePubSubAdapter studentVlePubSubAdapter;
    private final VlePubSubAdapter vlePubSubAdapter;

    private final ObjectMapper objectMapper;
    private final Map<String, Map<String, String>> googleCloudFileConfigs;

    @Autowired
    public PubSubServiceImpl(StudentAssessmentPubSubAdapter studentAssessmentPubSubAdapter, StudentInfoPubSubAdapter studentInfoPubSubAdapter, StudentRegistrationPubSubAdapter studentRegistrationPubSubAdapter, StudentVlePubSubAdapter studentVlePubSubAdapter, VlePubSubAdapter vlePubSubAdapter, ObjectMapper objectMapper, Map<String,
            Map<String, String>> googleCloudFileConfigs) {
        this.studentAssessmentPubSubAdapter = studentAssessmentPubSubAdapter;
        this.studentInfoPubSubAdapter = studentInfoPubSubAdapter;
        this.studentRegistrationPubSubAdapter = studentRegistrationPubSubAdapter;
        this.studentVlePubSubAdapter = studentVlePubSubAdapter;
        this.vlePubSubAdapter = vlePubSubAdapter;
        this.objectMapper = objectMapper;
        this.googleCloudFileConfigs = googleCloudFileConfigs;
    }

    @Override
    public void publishVLEData(String fileLocation) throws FileNotFoundException, ClassNotFoundException {

        // fileLocation = des_raw_csv/1_assessments.csv
        // fileLocation = des_raw_csv/assessments_20230401.csv
        String detectFile = fileLocation.substring(fileLocation.lastIndexOf('/') + 1);
        Map<String, String> downloadedFileAndClassInfo = googleCloudFileConfigs.get(detectFile.substring(0, 1));
        String fileDownloadedPath = downloadedFileAndClassInfo.get("downloadedFileName")
                + fileLocation.substring(fileLocation.lastIndexOf('/') + 1);

        List<?> list = readFile(fileDownloadedPath, Class.forName(downloadedFileAndClassInfo.get("className")));
        for (Object o : list) {
            String strData;
            try {
                strData = this.objectMapper.writeValueAsString(o);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if (o instanceof StudentAssessment) {
                studentAssessmentPubSubAdapter.publish(strData);
            } else if (o instanceof StudentInfo) {
                studentInfoPubSubAdapter.publish(strData);
            } else if (o instanceof StudentRegistration) {
                studentRegistrationPubSubAdapter.publish(strData);
            } else if (o instanceof StudentVle) {
                studentVlePubSubAdapter.publish(strData);
            } else if (o instanceof Vle) {
                vlePubSubAdapter.publish(strData);
            } else {
                log.info("Doing nothing");
            }
        }
    }

    private <T> List<T> readFile(String data, Class<T> clazz) throws FileNotFoundException {
        log.info(data);
        return new CsvToBeanBuilder(new FileReader(data)).withSkipLines(1).withType(clazz).build().parse();
    }
}
