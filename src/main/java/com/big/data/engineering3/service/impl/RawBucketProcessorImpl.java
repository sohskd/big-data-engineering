package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.BucketAdapter;
import com.big.data.engineering3.events.spring.TriggerEvent;
import com.big.data.engineering3.service.BucketProcessor;
import com.big.data.engineering3.service.PubSubService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static com.big.data.engineering3.constant.FileConstants.DOWNLOADED_PATH;

@Slf4j
@Service("raw")
public class RawBucketProcessorImpl implements BucketProcessor {

    private final PubSubService pubSubService;
    private final BucketPortService gsBucketService;
    private final BucketAdapter bucketAdapter;

    @Autowired
    public RawBucketProcessorImpl(PubSubService pubSubService, BucketPortService gsBucketService, BucketAdapter bucketAdapter) {
        this.pubSubService = pubSubService;
        this.gsBucketService = gsBucketService;
        this.bucketAdapter = bucketAdapter;
    }

    @SneakyThrows
    @Override
    public void process(TriggerEvent event) {

        String fileLocation = event.getName();

        // ignore fileLocation = des_raw_csv/1_assessments.csv
        // ignore fileLocation = des_raw_csv/3_courses.csv
        String fileNumber = fileLocation.substring(fileLocation.lastIndexOf('/') + 1).substring(0, 1);
        if (fileNumber.equals("1") || fileNumber.equals("3")) {
            log.info(String.format("Ignoring processing of %s.", fileLocation));
            return;
        }

        if (!Files.exists(Paths.get(String.format(DOWNLOADED_PATH, fileLocation)))) {
            val blob = this.bucketAdapter.downloadBlobFromRawZone(fileLocation);

            // only file 2, 4, 5 will be processed by dataflow jobs
            if (fileNumber.equals("2") || fileNumber.equals("4") || fileNumber.equals("5")) {
                this.pubSubService.publishVLEData(fileLocation);
            }

            // fileLocation = des_raw_csv/6_studentVle.csv
            // fileLocation = des_raw_csv/7_vle.csv
            // upload file manually due to not enough quota for dataflow jobs
            if (fileNumber.equals("6") || fileNumber.equals("7")) {
                this.gsBucketService.writeToLandingBucket(Collections.singletonList(blob));
            }

        } else {
            log.info(String.format("%s already exist on local.", fileLocation));
        }
    }
}
