package com.big.data.engineering3.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface MockDAO {
	public List<Map<String, Object>> getCourseByDelta(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByDelta(Timestamp delta) throws Exception;

}