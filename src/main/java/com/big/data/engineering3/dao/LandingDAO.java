package com.big.data.engineering3.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.big.data.engineering3.constant.SQLConstants;

public interface LandingDAO {
	
	public List<Map<String, Object>> getDelta() throws Exception;

	public int updateDelta(String tableName);

	public int insertAssessments(List<Map<String, Object>> mockAssessmentsDelta) throws Exception;
	
	public int insertCourses(List<Map<String, Object>> mockCoursesDelta) throws Exception;

	public List<Map<String, Object>> getCourseByDelta(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByDelta(Timestamp delta) throws Exception;
}