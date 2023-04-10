package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.BucketAdapter;
import com.big.data.engineering3.events.spring.TriggerEvent;
import com.big.data.engineering3.service.BucketProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;

import static com.big.data.engineering3.constant.FileConstants.DOWNLOADED_PATH_LANDING;
import static com.big.data.engineering3.utils.FileUtils.getFileName;

@Slf4j
@Service("landing")
public class LandingBucketProcessorImpl implements BucketProcessor {

    private final BucketAdapter bucketAdapter;

    @Autowired
    public LandingBucketProcessorImpl(BucketAdapter bucketAdapter) {
        this.bucketAdapter = bucketAdapter;
    }

    @Override
    public void process(TriggerEvent event) {

        String fileLocation = event.getName();
        if (!Files.exists(Paths.get(String.format(DOWNLOADED_PATH_LANDING, getFileName(fileLocation))))) {
            this.bucketAdapter.downloadBlobFromLandingZone(fileLocation);
        } else {
            log.info(String.format("%s already exist on local.", fileLocation));
        }
    }
}
