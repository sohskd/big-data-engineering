package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.GoldDAO;
import com.big.data.engineering3.dao.LandingDAO;
import com.big.data.engineering3.dao.MockDAO;
import com.big.data.engineering3.dao.SensitiveDAO;
import com.big.data.engineering3.dao.WorkDAO;
import com.big.data.engineering3.service.BatchJobService;
import com.big.data.engineering3.service.SparkJobService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BatchJobServiceImpl implements BatchJobService {
	
	@Autowired
	MockDAO mockDAO;
	
	@Autowired
	LandingDAO landingDAO;
	
	@Autowired
	GoldDAO goldDAO;
	
	@Autowired
	WorkDAO workDAO;
	
	@Autowired
	SensitiveDAO sensitiveDAO;
    
	@Autowired
    private SparkJobService sparkJobService;
	
	@Autowired
    public BatchJobServiceImpl() {
    }

//    @Scheduled(fixedDelay = 100000)//100sec
//    @Override
//    public void process() {
//        log.info("Running cron");
//        try {
//        	
//        	processMockZoneToLandingZone();
//        	processLandingZoneToGoldZone();
//        	List<String> errorList = new ArrayList<String>();
//        	sparkJobService.ingest_studentAssessment(errorList);
//        	sparkJobService.ingest_studentInfo(errorList);
//        	sparkJobService.ingest_studentRegistration(errorList);
//        	sparkJobService.ingest_studentVle(errorList);
//        	sparkJobService.ingest_vle(errorList);
//        	log.info(errorList.toString());
//        } catch(Exception e) {
//        	throw new RuntimeException(e);
//        }
//        
//        log.info("Done cron");
//    }
    
    public Map<String,Timestamp> formatDeltaMap(List<Map<String, Object>> map) {
		UnaryOperator<Map<String, Object>> formatDelta = (row) -> {
			log.info("Mapping :: table_name :: " + row.get("table_name"));
			row.put("table_name", (String) row.get("table_name"));
			row.put("time_executed", (Timestamp) row.get("time_executed"));
			return row;
		};
		log.info("formatDeltaMap :: size :: " + map.size());
		return map.stream().map(formatDelta)
				.collect(Collectors.toMap(m -> (String) m.get("table_name"), m -> (Timestamp) m.get("time_executed")));
	}
    @Transactional
    public void processMockZoneToLandingZone() {
        log.info("Running processMockZoneToLandingZone");
        try {
        	
        	log.info("Querying :: Last Import from landingDB Delta");
        	Map<String,Timestamp> landingDelta = formatDeltaMap(landingDAO.getDelta());
        	//Assessments
        	log.info("Querying :: Delta assessments from mockDB");
        	
        	List<Map<String, Object>> mockAssessmentsDeltaInsert = mockDAO.getAssessmentsByINSERTTIMESTAMP(landingDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	mockAssessmentsDeltaInsert.forEach(m->log.info(m.values().toString()));
        	int insertedLandingAssessments = landingDAO.insertAssessments(mockAssessmentsDeltaInsert);
        	log.info("insertedLandingAssessments :: " + insertedLandingAssessments);
        	
        	List<Map<String, Object>> mockAssessmentsDeltaUpdate = mockDAO.getAssessmentsByCHANGETIMESTAMP(landingDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	mockAssessmentsDeltaUpdate.forEach(m->log.info(m.values().toString()));
        	int updatedLandingAssessments = landingDAO.updateAssessments(mockAssessmentsDeltaUpdate);
        	log.info("updatedLandingAssessments :: " + updatedLandingAssessments);
        	
        	int updateLandingDeltaForAssessments = landingDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
        	log.info("updateLandingDeltaForAssessments :: " + updateLandingDeltaForAssessments);// Note: delta time is delayed 
        	
        	
        	//Courses
        	log.info("Querying :: Delta courses from mockDB");
        	
        	List<Map<String, Object>> mockCoursesDeltaInsert = mockDAO.getCourseByINSERTTIMESTAMP(landingDelta.get(SQLConstants.TABLE_COURSES));
        	mockCoursesDeltaInsert.forEach(m->log.info(m.values().toString()));
        	int insertedLandingCourses = landingDAO.insertCourses(mockCoursesDeltaInsert);
        	log.info("insertedLandingCourses :: " + insertedLandingCourses);
        	
        	List<Map<String, Object>> mockCoursesDeltaUpdate = mockDAO.getCourseByCHANGETIMESTAMP(landingDelta.get(SQLConstants.TABLE_COURSES));
        	mockCoursesDeltaUpdate.forEach(m->log.info(m.values().toString()));
        	int updatedLandingCourses = landingDAO.updateCourses(mockCoursesDeltaUpdate);
        	log.info("updatedLandingCourses :: " + updatedLandingCourses);
        	
        	int updateLandingDeltaForCourses = landingDAO.updateDelta(SQLConstants.TABLE_COURSES);
        	log.info("updateLandingDeltaForCourses :: " + updateLandingDeltaForCourses);// Note: delta time is delayed 

        	
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done processMockZoneToLandingZone");
    }
    
    @Transactional
    public void processLandingZoneToGoldZone() {
        log.info("Running processLandingZoneToGoldZone");
        try {
        	
        	log.info("Querying :: Last Import from goldDB Delta");
        	Map<String,Timestamp> goldDelta = formatDeltaMap(goldDAO.getDelta());
        	//Assessments
        	log.info("Querying :: Delta assessments from landingDB");
        	
        	List<Map<String, Object>> landingAssessmentsDeltaInsert = landingDAO.getAssessmentsByINSERTTIMESTAMP(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	landingAssessmentsDeltaInsert.forEach(m->log.info(m.values().toString()));
        	int insertedGoldAssessments = goldDAO.insertAssessments(landingAssessmentsDeltaInsert);
        	log.info("insertedGoldAssessments :: " + insertedGoldAssessments);
        	int updateGoldDeltaForAssessments = goldDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
        	
        	List<Map<String, Object>> landingAssessmentsDeltaUpdate = landingDAO.getAssessmentsByCHANGETIMESTAMP(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	landingAssessmentsDeltaUpdate.forEach(m->log.info(m.values().toString()));
        	int updatedGoldAssessments = goldDAO.updateAssessments(landingAssessmentsDeltaUpdate);
        	log.info("updatedGoldAssessments :: " + updatedGoldAssessments);
        	
        	log.info("updateGoldDeltaForAssessments :: " + updateGoldDeltaForAssessments);// Note: delta time is delayed 
        	
        	
        	//Courses
        	log.info("Querying :: Delta courses from landingDB");
        	
        	List<Map<String, Object>> landingCoursesDeltaInsert = landingDAO.getCourseByINSERTTIMESTAMP(goldDelta.get(SQLConstants.TABLE_COURSES));
        	landingCoursesDeltaInsert.forEach(m->log.info(m.values().toString()));
        	int insertedGoldCourses = goldDAO.insertCourses(landingCoursesDeltaInsert);
        	log.info("insertedGoldCourses :: " + insertedGoldCourses);
        	
          	List<Map<String, Object>> landingCoursesDeltaUpdate = landingDAO.getCourseByCHANGETIMESTAMP(goldDelta.get(SQLConstants.TABLE_COURSES));
        	landingCoursesDeltaUpdate.forEach(m->log.info(m.values().toString()));
        	int updatedGoldCourses = goldDAO.updateCourses(landingCoursesDeltaUpdate);
        	log.info("updatedGoldCourses :: " + updatedGoldCourses);
        	
        	int updateGoldDeltaForCourses = goldDAO.updateDelta(SQLConstants.TABLE_COURSES);
        	
  
        	
        	
        	log.info("updateGoldDeltaForCourses :: " + updateGoldDeltaForCourses);// Note: delta time is delayed 

        	
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done processLandingZoneToGoldZone");
    }

}