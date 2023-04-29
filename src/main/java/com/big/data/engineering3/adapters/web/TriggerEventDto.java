package com.big.data.engineering3.adapters.web;


import com.big.data.engineering3.events.spring.TriggerEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TriggerEventDto {

    private String name;
    private String bucket;

    public static TriggerEvent dto2Domain(TriggerEventDto eventDto) {
        return new TriggerEvent(new Object(), eventDto.getName(), eventDto.getBucket());
    }
}
