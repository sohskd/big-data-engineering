package com.big.data.engineering3.domain;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class Assessment {

    @CsvBindByPosition(position = 0)
    private String codeModule;

    @CsvBindByPosition(position = 1)
    private String codePresentation;

    @CsvBindByPosition(position = 2)
    private String idAssessment;

    @CsvBindByPosition(position = 3)
    private String assessmentType;

    @CsvBindByPosition(position = 4)
    private String date;

    @CsvBindByPosition(position = 5)
    private String weight;
}
