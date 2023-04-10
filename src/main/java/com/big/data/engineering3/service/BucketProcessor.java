package com.big.data.engineering3.service;

import com.big.data.engineering3.events.spring.TriggerEvent;

public interface BucketProcessor {

    void process(TriggerEvent event);
}
