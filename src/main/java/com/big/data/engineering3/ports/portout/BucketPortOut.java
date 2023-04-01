package com.big.data.engineering3.ports.portout;

import com.google.cloud.storage.Blob;

import java.util.List;

public interface BucketPortOut {

    void writeToLandingBucket(List<Blob> blobList);
}
