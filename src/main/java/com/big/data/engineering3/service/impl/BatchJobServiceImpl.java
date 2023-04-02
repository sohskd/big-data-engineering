package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.service.BatchJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BatchJobServiceImpl implements BatchJobService {
	
	@Autowired
	@Qualifier("mockJdbcTemplate")
	JdbcTemplate mockJdbcTemplate;
	
	@Autowired
	@Qualifier("landingJdbcTemplate")
	JdbcTemplate landingJdbcTemplate;
	
	@Autowired
	@Qualifier("goldJdbcTemplate")
	JdbcTemplate goldJdbcTemplate;
	
	@Autowired
	@Qualifier("workJdbcTemplate")
	JdbcTemplate workJdbcTemplate;
	@Autowired
	@Qualifier("sensitiveJdbcTemplate")
	JdbcTemplate sensitiveJdbcTemplate;
	
	
    @Autowired
    public BatchJobServiceImpl() {
    }

    @Scheduled(fixedDelay = 100000)//100sec
    @Override
    public void process() {
        log.info("Running cron");
        try {
        	log.info("Querying Test :: mockdb");
        	String mockStmt = String.format("select * from courses order by code_module limit 3;");
        	List<String> mockResult = this.mockJdbcTemplate.queryForList(mockStmt)
        							.stream().map((m) -> m.values().toString()).collect(Collectors.toList());
        	mockResult.forEach(x->log.info(x));
        	log.info("Querying Test :: landingdb");
        	String landingStmt = String.format("select * from courses order by code_module limit 3;");
        	List<String> landingResult = this.landingJdbcTemplate.queryForList(landingStmt)
        							.stream().map((m) -> m.values().toString()).collect(Collectors.toList());
        	landingResult.forEach(x->log.info(x));
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done cron");
    }
}