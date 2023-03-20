package com.big.data.engineering3.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VLEDataDomain {

    private List<StudentAssessment> studentAssessmentList;
}
