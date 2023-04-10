package com.big.data.engineering3.dao.impl;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.BaseDAO;
import com.big.data.engineering3.dao.MockDAO;
import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Repository
public class MockDAOImpl implements MockDAO {
	@Autowired
	@Qualifier("mockJdbcTemplate")
	JdbcTemplate mockJdbcTemplate;

	public JdbcTemplate getMockJdbcTemplate() {
		return mockJdbcTemplate;
	}

	public void setMockJdbcTemplate(JdbcTemplate mockJdbcTemplate) {
		this.mockJdbcTemplate = mockJdbcTemplate;
	}
	
	

	@Override
	public List<Map<String, Object>> getAssessmentsByINSERTTIMESTAMP(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getAssessmentsByINSERTTIMESTAMP");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_ASSESSMENTS_BY_INSERTTIMESTAMP,new Object[] {delta});
	}

	@Override
	public List<Map<String, Object>> getAssessmentsByCHANGETIMESTAMP(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getAssessmentsByCHANGETIMESTAMP");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_ASSESSMENTS_BY_CHANGETIMESTAMP,new Object[] {delta});
	}
	
	public List<Map<String, Object>> getCourseByINSERTTIMESTAMP(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getCourseByINSERTTIMESTAMP");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_COURSES_BY_INSERTTIMESTAMP,new Object[] {delta});
	}
	
	@Override
	public List<Map<String, Object>> getCourseByCHANGETIMESTAMP(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getCourseByCHANGETIMESTAMP");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_COURSES_BY_CHANGETIMESTAMP,new Object[] {delta});
	}
}
