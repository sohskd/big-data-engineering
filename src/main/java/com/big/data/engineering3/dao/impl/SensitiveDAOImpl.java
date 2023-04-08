package com.big.data.engineering3.dao.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.big.data.engineering3.dao.BaseDAO;
import com.big.data.engineering3.dao.SensitiveDAO;
import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Repository
public class SensitiveDAOImpl implements SensitiveDAO {
	@Autowired
	@Qualifier("sensitiveJdbcTemplate")
	JdbcTemplate sensitiveJdbcTemplate;

	public JdbcTemplate getSensitiveJdbcTemplate() {
		return sensitiveJdbcTemplate;
	}

	public void setSensitiveJdbcTemplate(JdbcTemplate sensitiveJdbcTemplate) {
		this.sensitiveJdbcTemplate = sensitiveJdbcTemplate;
	}
}
