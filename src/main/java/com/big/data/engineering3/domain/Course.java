package com.big.data.engineering3.domain;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class Course {

    @CsvBindByPosition(position = 0)
    private String codeModule;

    @CsvBindByPosition(position = 1)
    private String codePresentation;

    @CsvBindByPosition(position = 2)
    private String modulePresentationLength;
}
