package com.big.data.engineering3.config.vledataset;

import com.big.data.engineering3.domain.StudentAssessment;
import com.big.data.engineering3.domain.VLEDataDomain;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class StudentAssessmentDataConfig {

    private VLEDataDomain vleDataDomain = new VLEDataDomain();

    @Bean
    VLEDataDomain readFiles(@Value("${data.student.assesment.location}") String dataStudentAssessmentLocation) throws FileNotFoundException {
        readFile(dataStudentAssessmentLocation);
        return vleDataDomain;
    }

    private void readFile(String dataStudentAssessmentLocation) throws FileNotFoundException {
        log.info(dataStudentAssessmentLocation);
        List<StudentAssessment> studentAssessmentList = new CsvToBeanBuilder(new FileReader(dataStudentAssessmentLocation))
                .withSkipLines(1)
                .withType(StudentAssessment.class)
                .build()
                .parse();
        vleDataDomain.setStudentAssessmentList(studentAssessmentList);
    }
}
