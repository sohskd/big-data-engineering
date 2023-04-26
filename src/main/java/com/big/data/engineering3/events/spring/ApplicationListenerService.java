package com.big.data.engineering3.events.spring;

import com.big.data.engineering3.adapters.web.TriggerEventDto;
import com.big.data.engineering3.service.BucketProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

@Slf4j
@Component
public class ApplicationListenerService implements ApplicationListener<TriggerEvent> {

    private Map<String, BucketProcessor> bucketProcessorMap;

    @Autowired
    public ApplicationListenerService(Map<String, BucketProcessor> bucketProcessorMap) {
        this.bucketProcessorMap = bucketProcessorMap;
    }

    @SneakyThrows
    @Override
    public void onApplicationEvent(TriggerEvent event) {

        if (event.getBucket().contains("raw")) {
            bucketProcessorMap.get("raw").process(event);
        } else if (event.getBucket().contains("landing")) {
            bucketProcessorMap.get("landing").process(event);
        } else {
            log.info("Unable to find bucket processor");
        }
    }
}
