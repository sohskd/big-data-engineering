package com.big.data.engineering3.events.spring;

import org.springframework.context.ApplicationEvent;

public class TriggerEvent extends ApplicationEvent {
    private final String name;

    public TriggerEvent(Object source, String message) {
        super(source);
        this.name = message;
    }

    public String getName() {
        return name;
    }
}
