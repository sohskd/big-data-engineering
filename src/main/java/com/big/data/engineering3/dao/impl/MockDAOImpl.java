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
	
	public List<Map<String, Object>> getCourseByDelta(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getCourseByDelta");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_COURSES_BY_DELTA,new Object[] {delta});
	}

	@Override
	public List<Map<String, Object>> getAssessmentsByDelta(Timestamp delta) throws Exception {
		log.info("MockDAOImpl :: getAssessmentsByDelta");
		return getMockJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_ASSESSMENTS_BY_DELTA,new Object[] {delta});
	}
}
