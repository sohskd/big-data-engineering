package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.adapters.BucketAdapter;
import com.big.data.engineering3.ports.portin.BucketPortIn;
import com.google.cloud.storage.Blob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Reference: https://github.com/spring-attic/spring-cloud-gcp/blob/main/spring-cloud-gcp-samples/spring-cloud-gcp-storage-resource-sample/src/main/java/com/example/WebController.java
 */
@Slf4j
@Configuration
public class BucketPortService implements BucketPortIn {

    BucketAdapter bucketAdapter;

    @Autowired
    public BucketPortService(BucketAdapter bucketAdapter) {
        this.bucketAdapter = bucketAdapter;
    }

    @Override
    public List<Blob> downloadBlobsFromRawBucket() {
        return bucketAdapter.downloadBlobsFromRawBucket();
    }


    public void writeToLandingBucket(List<Blob> blobList) {
        bucketAdapter.writeToLandingBucket(blobList);
    }
}
