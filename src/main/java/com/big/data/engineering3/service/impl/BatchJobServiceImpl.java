package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.GoldDAO;
import com.big.data.engineering3.dao.LandingDAO;
import com.big.data.engineering3.dao.MockDAO;
import com.big.data.engineering3.dao.SensitiveDAO;
import com.big.data.engineering3.dao.WorkDAO;
import com.big.data.engineering3.service.BatchJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
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
    public BatchJobServiceImpl() {
    }

    @Scheduled(fixedDelay = 100000)//100sec
    @Override
    public void process() {
        log.info("Running cron");
        try {
        	
        	processMockZoneToLandingZone();
        	processLandingZoneToGoldZone();
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done cron");
    }
    
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
        	
        	List<Map<String, Object>> mockAssessmentsDelta = mockDAO.getAssessmentsByDelta(landingDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	mockAssessmentsDelta.forEach(m->log.info(m.values().toString()));
        	int insertedLandingAssessments = landingDAO.insertAssessments(mockAssessmentsDelta);
        	log.info("insertedLandingAssessments :: " + insertedLandingAssessments);
        	int updateLandingDeltaForAssessments = landingDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
        	log.info("updateLandingDeltaForAssessments :: " + updateLandingDeltaForAssessments);// Note: delta time is delayed 
        	
        	
        	//Courses
        	log.info("Querying :: Delta courses from mockDB");
        	
        	List<Map<String, Object>> mockCoursesDelta = mockDAO.getCourseByDelta(landingDelta.get(SQLConstants.TABLE_COURSES));
        	mockCoursesDelta.forEach(m->log.info(m.values().toString()));
        	int insertedLandingCourses = landingDAO.insertCourses(mockCoursesDelta);
        	log.info("insertedLandingCourses :: " + insertedLandingCourses);
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
        	
        	List<Map<String, Object>> landingAssessmentsDelta = landingDAO.getAssessmentsByDelta(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	landingAssessmentsDelta.forEach(m->log.info(m.values().toString()));
        	int insertedGoldAssessments = goldDAO.insertAssessments(landingAssessmentsDelta);
        	log.info("insertedGoldAssessments :: " + insertedGoldAssessments);
        	int updateGoldDeltaForAssessments = goldDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
        	log.info("updateGoldDeltaForAssessments :: " + updateGoldDeltaForAssessments);// Note: delta time is delayed 
        	
        	
        	//Courses
        	log.info("Querying :: Delta courses from landingDB");
        	
        	List<Map<String, Object>> landingCoursesDelta = landingDAO.getCourseByDelta(goldDelta.get(SQLConstants.TABLE_COURSES));
        	landingCoursesDelta.forEach(m->log.info(m.values().toString()));
        	int insertedGoldCourses = goldDAO.insertCourses(landingCoursesDelta);
        	log.info("insertedGoldCourses :: " + insertedGoldCourses);
        	int updateGoldDeltaForCourses = goldDAO.updateDelta(SQLConstants.TABLE_COURSES);
        	log.info("updateGoldDeltaForCourses :: " + updateGoldDeltaForCourses);// Note: delta time is delayed 

        	
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done processLandingZoneToGoldZone");
    }

}