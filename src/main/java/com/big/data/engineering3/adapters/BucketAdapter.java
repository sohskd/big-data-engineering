package com.big.data.engineering3.adapters;

import com.big.data.engineering3.ports.portin.BucketPortIn;
import com.big.data.engineering3.ports.portout.BucketPortOut;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.big.data.engineering3.constant.FileConstants.*;
import static com.big.data.engineering3.utils.FileUtils.getFileName;

@Slf4j
@Service
public class BucketAdapter implements BucketPortIn, BucketPortOut {

    @Value("${gcp.project.id}")
    private String gcpProjectId;

    @Value("${gcp.bucket.id}")
    private String gcpBucketLandingId;

    @Value("${gcp.bucket.raw.id}")
    private String gcpBucketRawId;

    @Autowired
    private Storage storage;

    @Override
    public List<Blob> downloadBlobsFromRawBucket() {
        List<Blob> listOfBlobs = getListOfBlobsStartWith(gcpProjectId, gcpBucketRawId, GCP_LOCATION);
        return downloadBlobs(listOfBlobs, DOWNLOADED_PATH);
    }

    @Override
    public Blob downloadBlobFromRawZone(String fileName) {
        log.info(String.format("Downloading %s from raw zone", fileName));
        val blob = storage.get(gcpBucketRawId, fileName);
        downloadBlobs(Collections.singletonList(blob), DOWNLOADED_PATH);
        return blob;
    }

    @Override
    public Blob downloadBlobFromLandingZone(String fileName) {
        log.info(String.format("Downloading %s from landing zone", fileName));
        val blob = storage.get(gcpBucketLandingId, fileName);
        downloadBlobsToLanding(Collections.singletonList(blob), DOWNLOADED_PATH_LANDING);
        return blob;
    }

    @Override
    public void writeToLandingBucket(List<Blob> blobList) {
        blobList.forEach(b -> {
            try {
                uploadObject(gcpProjectId, gcpBucketLandingId, b.getName(), String.format(DOWNLOADED_PATH, b.getName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void uploadObject(String projectId, String bucketName, String objectName, String filePath) throws IOException {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        // Optional: set a generation-match precondition to avoid potential race
        // conditions and data corruptions. The request returns a 412 error if the
        // preconditions are not met.
        Storage.BlobWriteOption precondition;
        if (storage.get(bucketName, objectName) == null) {
            // For a target object that does not yet exist, set the DoesNotExist precondition.
            // This will cause the request to fail if the object is created before the request runs.
            precondition = Storage.BlobWriteOption.doesNotExist();
            storage.createFrom(blobInfo, Paths.get(filePath), precondition);
            log.info(String.format("Successfully wrote to %s", filePath));
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition = Storage.BlobWriteOption.generationMatch();
            log.info(String.format("Skipping uploading to %s", filePath));
        }
    }

    private List<Blob> downloadBlobsToLanding(List<Blob> listOfBlobs, String location) {
        List<Blob> blobsToDownload = listOfBlobs.stream().filter(b -> {
            Path p = Paths.get(String.format(location, b.getName()));
            return !Files.exists(p);
        }).toList();

        blobsToDownload.forEach(b -> b.downloadTo(Paths.get(String.format(location,
                getFileName(b.getName()).replace(":", "-")))));
        return blobsToDownload;
    }

    private List<Blob> downloadBlobs(List<Blob> listOfBlobs, String location) {
        List<Blob> blobsToDownload = listOfBlobs.stream().filter(b -> {
            Path p = Paths.get(String.format(location, b.getName()));
            return !Files.exists(p);
        }).toList();

        blobsToDownload.forEach(b -> b.downloadTo(Paths.get(String.format(location, b.getName()))));
        return blobsToDownload;
    }

    private List<Blob> getListOfBlobsStartWith(String projectId, String bucketName, String startingWith) {

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Blob> blobs = storage.list(bucketName);

        return StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                .filter(blob -> blob.getName().startsWith(startingWith) && blob.getName().length() > startingWith.length())
                .collect(Collectors.toList());
    }
}
