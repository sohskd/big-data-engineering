package com.big.data.engineering3.service;

import com.google.cloud.storage.Blob;

import java.util.List;

public interface PubSubLoader {

    void publishData(List<Blob> blobList);
}
