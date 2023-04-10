package com.big.data.engineering3.dao.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.BaseDAO;
import com.big.data.engineering3.dao.GoldDAO;
import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Repository
public class GoldDAOImpl implements GoldDAO {
	@Autowired
	@Qualifier("goldJdbcTemplate")
	JdbcTemplate goldJdbcTemplate;

	public JdbcTemplate getGoldJdbcTemplate() {
		return goldJdbcTemplate;
	}

	public void setGoldJdbcTemplate(JdbcTemplate goldJdbcTemplate) {
		this.goldJdbcTemplate = goldJdbcTemplate;
	}
	

	public List<Map<String, Object>> getDelta() throws Exception {
		log.info("GoldDAOImpl :: getDelta");
		return getGoldJdbcTemplate().queryForList(SQLConstants.QUERY_SELECT_DELTA);
	}
	@Override
	public int updateDelta(String tableName) {
		log.info("GoldDAOImpl :: updateDelta");
		return getGoldJdbcTemplate().update(SQLConstants.QUERY_UPDATE_DELTA, tableName);
	}
	
	@Override
	public int insertAssessments(List<Map<String, Object>> landingAssessmentsDeltaInsert) throws Exception {
		log.info("GoldDAOImpl :: insertAssessments");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
            	ps.setString(1, (String) row.get("code_module"));
                ps.setString(2, (String) row.get("code_presentation"));
				ps.setString(3, (String) row.get("assessment_type"));
				ps.setObject(4, StringUtils.isBlank((String) row.get("date"))? null: Integer.parseInt((String) row.get("date")));
				ps.setObject(5, StringUtils.isBlank((String) row.get("weight"))? null: Double.parseDouble((String) row.get("weight")));
				ps.setObject(6, StringUtils.isBlank((String) row.get("id_assessment"))? null: Integer.parseInt((String) row.get("id_assessment")));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsertOrUpdate(getGoldJdbcTemplate(), SQLConstants.QUERY_INSERT_ASSESSMENTS, landingAssessmentsDeltaInsert, dataMapping);
	}

	@Override
	public int updateAssessments(List<Map<String, Object>> landingAssessmentsDeltaUpdate) throws Exception {
		log.info("GoldDAOImpl :: updateAssessments");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
            	ps.setString(1, (String) row.get("code_module"));
                ps.setString(2, (String) row.get("code_presentation"));
				ps.setString(3, (String) row.get("assessment_type"));
				ps.setObject(4, StringUtils.isBlank((String) row.get("date"))? null: Integer.parseInt((String) row.get("date")));
				ps.setObject(5, StringUtils.isBlank((String) row.get("weight"))? null: Double.parseDouble((String) row.get("weight")));
				ps.setObject(6, StringUtils.isBlank((String) row.get("id_assessment"))? null: Integer.parseInt((String) row.get("id_assessment")));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsertOrUpdate(getGoldJdbcTemplate(), SQLConstants.QUERY_UPDATE_ASSESSMENTS, landingAssessmentsDeltaUpdate, dataMapping);
	}
	
	@Override
	public int insertCourses(List<Map<String, Object>> landingCoursesDeltaInsert) throws Exception {
		log.info("GoldDAOImpl :: insertCourses");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
            	ps.setString(1, (String) row.get("code_module"));
                ps.setString(2, (String) row.get("code_presentation"));
				ps.setObject(3, StringUtils.isBlank((String) row.get("module_presentation_length"))? null: Integer.parseInt((String) row.get("module_presentation_length")));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsertOrUpdate(getGoldJdbcTemplate(), SQLConstants.QUERY_INSERT_COURSES, landingCoursesDeltaInsert, dataMapping);
	}
	@Override
	public int updateCourses(List<Map<String, Object>> landingCoursesDeltaUpdate) throws Exception {
		log.info("GoldDAOImpl :: updateCourses");
    	BiConsumer<Map<String, Object>,PreparedStatement> dataMapping  = (row,ps) -> {
            try {
				ps.setObject(1, StringUtils.isBlank((String) row.get("module_presentation_length"))? null: Integer.parseInt((String) row.get("module_presentation_length")));
				ps.setString(2, (String) row.get("code_module"));
            	ps.setString(3, (String) row.get("code_presentation"));
            } catch (SQLException e) {
				throw new RuntimeException(e);
			}
    	};
		return BaseDAO.batchInsertOrUpdate(getGoldJdbcTemplate(), SQLConstants.QUERY_UPDATE_COURSES, landingCoursesDeltaUpdate, dataMapping);
	}
}
