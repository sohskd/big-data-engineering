package com.big.data.engineering3.events.spring;

import com.big.data.engineering3.adapters.BucketAdapter;
import com.big.data.engineering3.service.PubSubService;
import com.big.data.engineering3.service.impl.BucketPortService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static com.big.data.engineering3.constant.FileConstants.DOWNLOADED_PATH;

@Slf4j
@Component
public class ApplicationListenerService implements ApplicationListener<TriggerEvent> {

    private final PubSubService pubSubService;
    private final BucketPortService gsBucketService;
    private final BucketAdapter bucketAdapter;

    @Autowired
    public ApplicationListenerService(PubSubService pubSubService, BucketPortService gsBucketService,
                                      BucketAdapter bucketAdapter) {
        this.pubSubService = pubSubService;
        this.gsBucketService = gsBucketService;
        this.bucketAdapter = bucketAdapter;
    }

    @SneakyThrows
    @Override
    public void onApplicationEvent(TriggerEvent event) {

        String fileLocation = event.getName();
        if (!Files.exists(Paths.get(String.format(DOWNLOADED_PATH, fileLocation)))) {
            val blob = this.bucketAdapter.downloadBlob(fileLocation);
            this.pubSubService.publishVLEData(fileLocation);
            this.gsBucketService.writeToLandingBucket(Collections.singletonList(blob));
        } else {
            log.info(String.format("%s already exist on local.", fileLocation));
        }
    }
}
