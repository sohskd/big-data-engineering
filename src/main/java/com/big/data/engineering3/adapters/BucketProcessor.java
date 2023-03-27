package com.big.data.engineering3.adapters;

import com.big.data.engineering3.service.GSBucketService;
import com.big.data.engineering3.service.PubSubLoader;
import com.google.cloud.storage.Blob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class BucketProcessor {

    private GSBucketService gsBucketService;
    private PubSubLoader pubSubLoader;

    @Autowired
    public BucketProcessor(GSBucketService gsBucketService, PubSubLoader pubSubLoader) {
        this.gsBucketService = gsBucketService;
        this.pubSubLoader = pubSubLoader;
    }

    @Scheduled(fixedDelay = 10000)
    public void cronRun() {
        log.info("Running cron");
        List<Blob> blobList = gsBucketService.downloadBlobsFromRawBucket();
        pubSubLoader.publishData(blobList);
        gsBucketService.writeToLandingBucket(blobList);
        log.info("Done cron");
    }
}
