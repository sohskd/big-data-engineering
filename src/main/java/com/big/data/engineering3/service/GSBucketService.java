package com.big.data.engineering3.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Reference: https://github.com/spring-attic/spring-cloud-gcp/blob/main/spring-cloud-gcp-samples/spring-cloud-gcp-storage-resource-sample/src/main/java/com/example/WebController.java
 */
@Slf4j
@Configuration
public class GSBucketService {

    //    @Value("gs://[YOUR_GCS_BUCKET]/[GCS_FILE_NAME]")
    @Value("gs://mock_raw_ebd_2023/20230101/studentAssessment.csv")
    private Resource gcsResource;

    @Value("gs://mock_raw_ebd_2023/20230101/studentAssessment1.csv")
    private Resource gcsResource1;

    @Value("${gcp.project.id}")
    private String gcpProjectId;

    @Value("${gcp.bucket.id}")
    private String gcpBucketLandingId;

    @Value("${gcp.bucket.raw.id}")
    private String gcpBucketRawId;

    public List<Blob> downloadBlobsFromRawBucket() {
        List<Blob> listOfBlobs = getListOfBlobsStartWith(gcpProjectId, gcpBucketRawId, "des_raw_csv/");
        return downloadBlobs(listOfBlobs);
    }

    private List<Blob> downloadBlobs(List<Blob> listOfBlobs) {
        List<Blob> blobsToDownload = listOfBlobs.stream().filter(b -> {
            Path p = Paths.get(String.format("data/downloaded/%s", b.getName()));
            return !Files.exists(p);
        }).toList();

        blobsToDownload.forEach(b -> b.downloadTo(Paths.get(String.format("data/downloaded/%s", b.getName()))));
        return blobsToDownload;
    }

    public void writeToLandingBucket(List<Blob> blobList) {
        blobList.forEach(b -> {
            try {
                uploadObject(gcpProjectId, gcpBucketLandingId, b.getName(), String.format("data/downloaded/%s", b.getName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void uploadObject(String projectId, String bucketName, String objectName, String filePath) throws IOException {
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
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition = Storage.BlobWriteOption.generationMatch();
        }
        storage.createFrom(blobInfo, Paths.get(filePath), precondition);
        log.info(String.format("Successfully wrote to %s", filePath));
    }

    public List<Blob> getListOfBlobsStartWith(String projectId, String bucketName, String startingWith) {

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Blob> blobs = storage.list(bucketName);

        return StreamSupport.stream(blobs.iterateAll().spliterator(), false).filter(blob -> blob.getName().startsWith(startingWith) && blob.getName().length() > startingWith.length()).collect(Collectors.toList());
    }
}
