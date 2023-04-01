package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.ports.portin.ProcessUseCase;
import com.big.data.engineering3.service.PubSubService;
import com.google.cloud.storage.Blob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ProcessorService implements ProcessUseCase {

    private BucketPortService gsBucketService;
    private PubSubService pubSubService;

    @Autowired
    public ProcessorService(BucketPortService gsBucketService, PubSubService pubSubService) {
        this.gsBucketService = gsBucketService;
        this.pubSubService = pubSubService;
    }

    @Scheduled(fixedDelay = 10000)
    @Override
    public void process() {
        log.info("Running cron");
        List<Blob> blobList = gsBucketService.downloadBlobsFromRawBucket();
        pubSubService.publishData(blobList);
        gsBucketService.writeToLandingBucket(blobList);
        log.info("Done cron");
    }
}
