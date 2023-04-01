package com.big.data.engineering3.ports.portout;

import org.springframework.integration.annotation.MessagingGateway;

public interface PubSubPortOut {

    void publish(String text);
}
