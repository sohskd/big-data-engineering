package com.big.data.engineering3.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface MockDAO {
	public List<Map<String, Object>> getCourseByINSERTTIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByINSERTTIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getAssessmentsByCHANGETIMESTAMP(Timestamp delta) throws Exception;

	public List<Map<String, Object>> getCourseByCHANGETIMESTAMP(Timestamp delta) throws Exception;

}