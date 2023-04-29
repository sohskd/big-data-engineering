package com.big.data.engineering3.adapters.web;

import com.big.data.engineering3.events.spring.ApplicationListenerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequestMapping("/api/")
@RestController
public class DownloadController {

    private final ApplicationListenerService applicationListenerService;

    @Autowired
    public DownloadController(ApplicationListenerService applicationListenerService) {
        this.applicationListenerService = applicationListenerService;
    }

    @PostMapping("trigger")
    public void parseTest(@RequestBody TriggerEventDto triggerEventDto) {

        log.info(String.valueOf(triggerEventDto));
        applicationListenerService.onApplicationEvent(TriggerEventDto.dto2Domain(triggerEventDto));
    }
}
