package com.big.data.engineering3.service;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.FileNotFoundException;

public interface PubSubService {

    void publishVLEData(String fileLocation) throws FileNotFoundException, ClassNotFoundException, JsonProcessingException;
}
