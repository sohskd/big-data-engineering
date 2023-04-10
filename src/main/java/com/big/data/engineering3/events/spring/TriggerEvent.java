package com.big.data.engineering3.events.spring;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class TriggerEvent extends ApplicationEvent {
    private final String name;
    private final String bucket;

    public TriggerEvent(Object source, String name, String bucket) {
        super(source);
        this.name = name;
        this.bucket = bucket;
    }
}
