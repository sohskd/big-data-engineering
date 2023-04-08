package com.big.data.engineering3.dao.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.big.data.engineering3.dao.BaseDAO;
import com.big.data.engineering3.dao.WorkDAO;
import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Repository
public class WorkDAOImpl implements WorkDAO {
	@Autowired
	@Qualifier("workJdbcTemplate")
	JdbcTemplate workJdbcTemplate;

	public JdbcTemplate getWorkJdbcTemplate() {
		return workJdbcTemplate;
	}

	public void setWorkJdbcTemplate(JdbcTemplate workJdbcTemplate) {
		this.workJdbcTemplate = workJdbcTemplate;
	}
}
