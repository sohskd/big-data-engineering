package com.big.data.engineering3.service;

import java.io.FileNotFoundException;

public interface PubSubService {

    void publishVLEData(String fileLocation) throws FileNotFoundException, ClassNotFoundException;
}
