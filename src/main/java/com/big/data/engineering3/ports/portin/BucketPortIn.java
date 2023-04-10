package com.big.data.engineering3.ports.portin;

import com.google.cloud.storage.Blob;

import java.util.List;

public interface BucketPortIn {

    List<Blob> downloadBlobsFromRawBucket();

    Blob downloadBlobFromRawZone(String fileName);

    Blob downloadBlobFromLandingZone(String fileName);
}
