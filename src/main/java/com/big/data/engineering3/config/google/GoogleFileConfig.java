package com.big.data.engineering3.config.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

@Configuration
public class GoogleFileConfig {

    @Value("${gcp.project.id}")
    private String gcpProjectId;

    @Bean
    public Map<String, Map<String, String>> googleCloudFileConfigs(ObjectMapper mapper) throws IOException {
        return mapper.readValue(loadFile(), Map.class);
    }

    @SneakyThrows
    @Bean
    public Storage storage() {
        File credentialsPath = ResourceUtils.getFile("classpath:gcs-service-account.json");
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        return StorageOptions.newBuilder().setCredentials(credentials).setProjectId(gcpProjectId).build().getService();
    }

    private File loadFile() throws FileNotFoundException {
        return ResourceUtils.getFile("classpath:google-files-config.json");
    }
}
