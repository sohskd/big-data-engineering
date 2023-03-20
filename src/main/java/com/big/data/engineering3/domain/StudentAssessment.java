package com.big.data.engineering3.domain;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class StudentAssessment {

    @CsvBindByPosition(position = 0)
    private String idAssessment;

    @CsvBindByPosition(position = 1)
    private String isStudent;

    @CsvBindByPosition(position = 2)
    private String dateSubmitted;

    @CsvBindByPosition(position = 3)
    private String isBanked;

    @CsvBindByPosition(position = 4)
    private String score;
}
