package com.big.data.engineering3.config.vledataset;

import com.big.data.engineering3.domain.Assessment;
import com.big.data.engineering3.domain.StudentAssessment;
import com.big.data.engineering3.dto.VLEDto;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

@Slf4j
@Configuration
public class StudentAssessmentDataConfig {

    @Bean
    VLEDto readFiles(@Value("${data.student.assesment.location}") String studentAssessmentLocation,
                     @Value("${data.assesment.location}") String assessmentLocation) throws FileNotFoundException {
        VLEDto vleDto = new VLEDto();
        vleDto.setStudentAssessmentList(readFile(studentAssessmentLocation, StudentAssessment.class));
        vleDto.setAssessmentList(readFile(assessmentLocation, Assessment.class));
        return vleDto;
    }

    private <T> List<T> readFile(String data, Class<T> clazz) throws FileNotFoundException {
        log.info(data);
        return new CsvToBeanBuilder(new FileReader(data))
                .withSkipLines(1)
                .withType(clazz)
                .build()
                .parse();
    }
}
