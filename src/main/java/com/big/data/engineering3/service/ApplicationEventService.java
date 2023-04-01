package com.big.data.engineering3.service;

import org.springframework.messaging.Message;

public interface ApplicationEventService {

    void publish(String payload, Message message);
}
