package com.big.data.engineering3.domain;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class Vle {

    @CsvBindByPosition(position = 0)
    private String idSite;

    @CsvBindByPosition(position = 1)
    private String codeModule;

    @CsvBindByPosition(position = 2)
    private String codePresentation;

    @CsvBindByPosition(position = 3)
    private String activityType;

    @CsvBindByPosition(position = 4)
    private String weekFrom;

    @CsvBindByPosition(position = 5)
    private String weekTo;
}