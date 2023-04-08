package com.big.data.engineering3.dao.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.BaseDAO;
import com.big.data.engineering3.dao.LandingDAO;
import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class LandingDAOImpl implements LandingDAO {
	@Autowired
	@Qualifier("landingJdbcTemplate")
	JdbcTemplate landingJdbcTemplate;

	public JdbcTemplate getLandingJdbcTemplate() {
		return landingJdbcTemplate;
	}

	public void setLandingJdbcTemplate(JdbcTemplate landingJdbcTemplate) {
		this.landingJdbcTemplate = landingJdbcTemplate;
	}
	
	public List<Map<String, Object>> getDelta() throws Exception {
		log.info("LandingDAOImpl :: getDelta");
		return getLandingJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_DELTA);
	}
	@Override
	public int updateDelta(String tableName) {
		log.info("LandingDAOImpl :: updateDelta");
		return getLandingJdbcTemplate().update(SQLConstants.QUERY_UPDATE_DELTA, tableName);
	}
	@Override
	public int insertCourses(List<Map<String, Object>> mockCoursesDelta) throws Exception {
		log.info("LandingDAOImpl :: insertCourses");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
            	ps.setString(1, (String) row.get("code_module"));
                ps.setString(2, (String) row.get("code_presentation"));
				ps.setString(3, (String) row.get("module_presentation_length"));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsert(getLandingJdbcTemplate(), SQLConstants.QUERY_INSERT_COURSES, mockCoursesDelta, dataMapping);
	}

	@Override
	public int insertAssessments(List<Map<String, Object>> mockAssessmentsDelta) throws Exception {
		log.info("LandingDAOImpl :: insertAssessments");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
            	ps.setString(1, (String) row.get("code_module"));
                ps.setString(2, (String) row.get("code_presentation"));
				ps.setString(3, (String) row.get("assessment_type"));
				ps.setString(4, (String) row.get("date"));
                ps.setString(5, (String) row.get("weight"));
				ps.setString(6, (String) row.get("id_assessment"));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsert(getLandingJdbcTemplate(), SQLConstants.QUERY_INSERT_ASSESSMENTS, mockAssessmentsDelta, dataMapping);
	}

	public List<Map<String, Object>> getCourseByDelta(Timestamp delta) throws Exception {
		log.info("LandingDAOImpl :: getCourseByDelta");
		return getLandingJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_COURSES_BY_DELTA,new Object[] {delta});
	}

	@Override
	public List<Map<String, Object>> getAssessmentsByDelta(Timestamp delta) throws Exception {
		log.info("LandingDAOImpl :: getAssessmentsByDelta");
		return getLandingJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_ASSESSMENTS_BY_DELTA,new Object[] {delta});
	}
	
}
