package com.big.data.engineering3.domain;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class StudentInfo {

    @CsvBindByPosition(position = 0)
    private String codeModule;

    @CsvBindByPosition(position = 1)
    private String codePresentation;

    @CsvBindByPosition(position = 2)
    private String idStudent;

    @CsvBindByPosition(position = 3)
    private String gender;

    @CsvBindByPosition(position = 4)
    private String region;

    @CsvBindByPosition(position = 5)
    private String highestEducation;

    @CsvBindByPosition(position = 6)
    private String imdBand;

    @CsvBindByPosition(position = 7)
    private String ageBand;

    @CsvBindByPosition(position = 8)
    private String numOfPrevAttempts;

    @CsvBindByPosition(position = 9)
    private String studiedCredits;

    @CsvBindByPosition(position = 10)
    private String disability;

    @CsvBindByPosition(position = 11)
    private String finalResult;
}