package com.big.data.engineering3.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.big.data.engineering3.constant.SQLConstants;

public interface LandingDAO {
	
	public List<Map<String, Object>> getDelta() throws Exception;

	public int updateDelta(String tableName);



	public List<Map<String, Object>> getCourseByINSERTTIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByINSERTTIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByCHANGETIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getCourseByCHANGETIMESTAMP(Timestamp delta) throws Exception;

	public int insertAssessments(List<Map<String, Object>> mockAssessmentsDeltaInsert) throws Exception;
	
	public int insertCourses(List<Map<String, Object>> mockCoursesDelta) throws Exception;
	
	public int updateAssessments(List<Map<String, Object>> mockAssessmentsDeltaUpdate) throws Exception;

	public int updateCourses(List<Map<String, Object>> mockCoursesDeltaUpdate) throws Exception;
}