package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.GoldDAO;
import com.big.data.engineering3.dao.LandingDAO;
import com.big.data.engineering3.dao.MockDAO;
import com.big.data.engineering3.dao.SensitiveDAO;
import com.big.data.engineering3.dao.WorkDAO;
import com.big.data.engineering3.service.BatchJobService;
import com.big.data.engineering3.service.SparkJobService;
import com.big.data.engineering3.spark.GoldSparkConfig;
import com.big.data.engineering3.spark.LandingSparkConfig;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
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
	private LandingSparkConfig landingSparkConfig;
	
	@Autowired
	private GoldSparkConfig goldSparkConfig;
	
	@Autowired
    private SparkJobService sparkJobService;
	
	@Autowired
    public BatchJobServiceImpl() {
    }

//    @Scheduled(fixedDelay = 60000)//60sec/same as window
    @Override
    public void process() {
        log.info("Running cron");
        try {
        	
//        	processMockZoneToLandingZone();
        	processLandingZoneToGoldZone();
        	List<String> errorList = new ArrayList<String>();
        	sparkJobService.ingest_studentAssessment(errorList);
        	sparkJobService.ingest_studentInfo(errorList);
        	sparkJobService.ingest_studentRegistration(errorList);
        	sparkJobService.ingest_studentVle(errorList);
        	sparkJobService.ingest_vle(errorList);
        	log.info(errorList.toString());
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
    
    
    /*@Deprecated
     * Replaced with CDC via Publication and Subscription
     * */
//    @Transactional
//    public void processMockZoneToLandingZone() {
//        log.info("Running processMockZoneToLandingZone");
//        try {
//        	
//        	log.info("Querying :: Last Import from landingDB Delta");
//        	Map<String,Timestamp> landingDelta = formatDeltaMap(landingDAO.getDelta());
//        	//Assessments
//        	log.info("Querying :: Delta assessments from mockDB");
//        	
//        	List<Map<String, Object>> mockAssessmentsDeltaInsert = mockDAO.getAssessmentsByINSERTTIMESTAMP(landingDelta.get(SQLConstants.TABLE_ASSESSMENTS));
//        	mockAssessmentsDeltaInsert.forEach(m->log.info(m.values().toString()));
//        	int insertedLandingAssessments = landingDAO.insertAssessments(mockAssessmentsDeltaInsert);
//        	log.info("insertedLandingAssessments :: " + insertedLandingAssessments);
//        	
//        	List<Map<String, Object>> mockAssessmentsDeltaUpdate = mockDAO.getAssessmentsByCHANGETIMESTAMP(landingDelta.get(SQLConstants.TABLE_ASSESSMENTS));
//        	mockAssessmentsDeltaUpdate.forEach(m->log.info(m.values().toString()));
//        	int updatedLandingAssessments = landingDAO.updateAssessments(mockAssessmentsDeltaUpdate);
//        	log.info("updatedLandingAssessments :: " + updatedLandingAssessments);
//        	
//        	int updateLandingDeltaForAssessments = landingDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
//        	log.info("updateLandingDeltaForAssessments :: " + updateLandingDeltaForAssessments);// Note: delta time is delayed 
//        	
//        	
//        	//Courses
//        	log.info("Querying :: Delta courses from mockDB");
//        	
//        	List<Map<String, Object>> mockCoursesDeltaInsert = mockDAO.getCourseByINSERTTIMESTAMP(landingDelta.get(SQLConstants.TABLE_COURSES));
//        	mockCoursesDeltaInsert.forEach(m->log.info(m.values().toString()));
//        	int insertedLandingCourses = landingDAO.insertCourses(mockCoursesDeltaInsert);
//        	log.info("insertedLandingCourses :: " + insertedLandingCourses);
//        	
//        	List<Map<String, Object>> mockCoursesDeltaUpdate = mockDAO.getCourseByCHANGETIMESTAMP(landingDelta.get(SQLConstants.TABLE_COURSES));
//        	mockCoursesDeltaUpdate.forEach(m->log.info(m.values().toString()));
//        	int updatedLandingCourses = landingDAO.updateCourses(mockCoursesDeltaUpdate);
//        	log.info("updatedLandingCourses :: " + updatedLandingCourses);
//        	
//        	int updateLandingDeltaForCourses = landingDAO.updateDelta(SQLConstants.TABLE_COURSES);
//        	log.info("updateLandingDeltaForCourses :: " + updateLandingDeltaForCourses);// Note: delta time is delayed 
//
//        	
//        } catch(Exception e) {
//        	throw new RuntimeException(e);
//        }
//        
//        log.info("Done processMockZoneToLandingZone");
//    }
    
    @Transactional
    public void processLandingZoneToGoldZone() {
        log.info("Running processLandingZoneToGoldZone");
        try {
        	
        	log.info("Querying :: Last Import from goldDB Delta");
        	Map<String,Timestamp> goldDelta = formatDeltaMap(goldDAO.getDelta());
        	//Assessments
        	log.info("Querying :: Delta assessments from landingDB");

			Dataset<Row> landingAssessmentsDeltaInsert = landingSparkConfig.readSession(SQLConstants.TABLE_ASSESSMENTS)
					.select("code_module","code_presentation","assessment_type","date","weight","id_assessment")
					.filter("inserttimestamp" + ">'" + goldDelta.get(SQLConstants.TABLE_ASSESSMENTS)+"'");
			landingAssessmentsDeltaInsert = landingAssessmentsDeltaInsert
					.withColumn("dateTemp", landingAssessmentsDeltaInsert.col("date").cast(DataTypes.IntegerType))
					.drop(landingAssessmentsDeltaInsert.col("date"))
					.withColumnRenamed("dateTemp", "date")
					.withColumn("weightTemp", landingAssessmentsDeltaInsert.col("weight").cast(DataTypes.FloatType))
					.drop(landingAssessmentsDeltaInsert.col("weight"))
					.withColumnRenamed("weightTemp", "weight")
					.withColumn("id_assessmentTemp", landingAssessmentsDeltaInsert.col("id_assessment").cast(DataTypes.IntegerType))
					.drop(landingAssessmentsDeltaInsert.col("id_assessment"))
					.withColumnRenamed("id_assessmentTemp", "id_assessment");

//			landingAssessmentsDeltaInsert.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_ASSESSMENTS, goldSparkConfig.goldConnectionProperties());
//        	List<Map<String, Object>> landingAssessmentsDeltaInsert = landingDAO.getAssessmentsByINSERTTIMESTAMP(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
//        	landingAssessmentsDeltaInsert.forEach(m->log.info(m.values().toString()));
//        	int insertedGoldAssessments = goldDAO.insertAssessments(landingAssessmentsDeltaInsert);
//        	log.info("insertedGoldAssessments :: " + insertedGoldAssessments);
        	
        	List<Map<String, Object>> landingAssessmentsDeltaUpdate = landingDAO.getAssessmentsByCHANGETIMESTAMP(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
        	landingAssessmentsDeltaUpdate.forEach(m->log.info(m.values().toString()));
        	int updatedGoldAssessments = goldDAO.updateAssessments(landingAssessmentsDeltaUpdate);
        	log.info("updatedGoldAssessments :: " + updatedGoldAssessments);
        	
        	
        	int updateGoldDeltaForAssessments = goldDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);

        	log.info("updateGoldDeltaForAssessments :: " + updateGoldDeltaForAssessments);// Note: delta time is delayed 
        	
        	
        	//Courses
        	log.info("Querying :: Delta courses from landingDB");
        	
        	
			Dataset<Row> landingCoursesDeltaInsert = landingSparkConfig.readSession(SQLConstants.TABLE_COURSES)
					.select("code_module","code_presentation","module_presentation_length")
					.filter("inserttimestamp" + ">'" + goldDelta.get(SQLConstants.TABLE_COURSES)+"'");
			landingCoursesDeltaInsert = landingCoursesDeltaInsert
					.withColumn("module_presentation_lengthTemp", landingCoursesDeltaInsert.col("module_presentation_length").cast(DataTypes.IntegerType))
					.drop(landingCoursesDeltaInsert.col("module_presentation_length"))
					.withColumnRenamed("module_presentation_lengthTemp", "module_presentation_length");
			landingCoursesDeltaInsert.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_COURSES, goldSparkConfig.goldConnectionProperties());
			
//        	List<Map<String, Object>> landingCoursesDeltaInsert = landingDAO.getCourseByINSERTTIMESTAMP(goldDelta.get(SQLConstants.TABLE_COURSES));
//        	landingCoursesDeltaInsert.forEach(m->log.info(m.values().toString()));
//        	int insertedGoldCourses = goldDAO.insertCourses(landingCoursesDeltaInsert);
//        	log.info("insertedGoldCourses :: " + insertedGoldCourses);
        	
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