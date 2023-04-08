package com.big.data.engineering3.dao;

import java.util.List;
import java.util.Map;

public interface GoldDAO {
	
	public List<Map<String, Object>> getDelta() throws Exception;

	public int updateDelta(String tableName);

	public int insertAssessments(List<Map<String, Object>> mockAssessmentsDelta) throws Exception;
	
	public int insertCourses(List<Map<String, Object>> mockCoursesDelta) throws Exception;

}