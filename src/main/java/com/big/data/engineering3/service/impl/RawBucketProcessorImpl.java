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
        if (!Files.exists(Paths.get(String.format(DOWNLOADED_PATH, fileLocation)))) {
            val blob = this.bucketAdapter.downloadBlobFromRawZone(fileLocation);
            this.pubSubService.publishVLEData(fileLocation);
            this.gsBucketService.writeToLandingBucket(Collections.singletonList(blob));
        } else {
            log.info(String.format("%s already exist on local.", fileLocation));
        }
    }
}
