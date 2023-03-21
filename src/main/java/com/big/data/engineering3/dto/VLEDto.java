package com.big.data.engineering3.dto;

import com.big.data.engineering3.domain.Assessment;
import com.big.data.engineering3.domain.StudentAssessment;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VLEDto {

    private List<StudentAssessment> studentAssessmentList;
    private List<Assessment> assessmentList;
}
