package com.big.data.engineering3.dao;

import java.util.List;
import java.util.Map;

public interface GoldDAO {
	
	public List<Map<String, Object>> getDelta() throws Exception;

	public int updateDelta(String tableName);

	public int insertAssessments(List<Map<String, Object>> landingAssessmentsDeltaInsert) throws Exception;
	
	public int insertCourses(List<Map<String, Object>> landingCoursesDelta) throws Exception;
	
	public int updateAssessments(List<Map<String, Object>> landingAssessmentsDeltaUpdate) throws Exception;

	public int updateCourses(List<Map<String, Object>> landingCoursesDeltaUpdate) throws Exception;


}