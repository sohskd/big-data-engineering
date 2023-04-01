package com.big.data.engineering3.config.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

@Configuration
public class GoogleFileConfig {

    @Bean
    public Map<String, Map<String, String>> googleCloudFileConfigs(ObjectMapper mapper) throws IOException {
        return mapper.readValue(loadFile(), Map.class);
    }

    private File loadFile() throws FileNotFoundException {
        return ResourceUtils.getFile("classpath:google-files-config.json");
    }
}
