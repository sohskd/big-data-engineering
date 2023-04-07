package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.service.BatchJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
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
	
	
    public JdbcTemplate getMockJdbcTemplate() {
		return mockJdbcTemplate;
	}

	public void setMockJdbcTemplate(JdbcTemplate mockJdbcTemplate) {
		this.mockJdbcTemplate = mockJdbcTemplate;
	}

	public JdbcTemplate getLandingJdbcTemplate() {
		return landingJdbcTemplate;
	}

	public void setLandingJdbcTemplate(JdbcTemplate landingJdbcTemplate) {
		this.landingJdbcTemplate = landingJdbcTemplate;
	}

	public JdbcTemplate getGoldJdbcTemplate() {
		return goldJdbcTemplate;
	}

	public void setGoldJdbcTemplate(JdbcTemplate goldJdbcTemplate) {
		this.goldJdbcTemplate = goldJdbcTemplate;
	}

	public JdbcTemplate getWorkJdbcTemplate() {
		return workJdbcTemplate;
	}

	public void setWorkJdbcTemplate(JdbcTemplate workJdbcTemplate) {
		this.workJdbcTemplate = workJdbcTemplate;
	}

	public JdbcTemplate getSensitiveJdbcTemplate() {
		return sensitiveJdbcTemplate;
	}

	public void setSensitiveJdbcTemplate(JdbcTemplate sensitiveJdbcTemplate) {
		this.sensitiveJdbcTemplate = sensitiveJdbcTemplate;
	}

	@Autowired
    public BatchJobServiceImpl() {
    }

    @Scheduled(fixedDelay = 100000)//100sec
    @Override
    public void process() {
        log.info("Running cron");
        try {
        	log.info("Querying :: mockdb");
        	String mockStmt = String.format("select * from courses order by code_module limit 3;");
        	List<String> mockResult = this.mockJdbcTemplate.queryForList(mockStmt)
        							.stream().map((m) -> m.values().toString()).collect(Collectors.toList());
        	mockResult.forEach(x->log.info(x));
        	
        	log.info("Querying :: landingDelta");
        	final String QUERY_SELECT_DELTA = String.format("select table_name, time_executed from delta;");
        	Map<String,Object> landingDelta = formatDeltaMap(this.landingJdbcTemplate.queryForList(QUERY_SELECT_DELTA));
        	log.info("course:: delta :: "+landingDelta.get("courses"));
        	
        	log.info("Querying :: mockLandingCourses");
        	///Mock to Landing
        	final String QUERY_SELECT_COURSES_DELTA = String.format("select code_module,code_presentation,module_presentation_length from courses where changetimestamp > ?;");
        	List<Map<String, Object>> mockLandingCourses = this.mockJdbcTemplate.queryForList(QUERY_SELECT_COURSES_DELTA,new Object[] {landingDelta.get("courses")});
        	mockLandingCourses.forEach(m->log.info(m.values().toString()));

        	BiConsumer<Map<String, Object>,PreparedStatement> queryMapping  = (row,ps) -> {
                try {
                	ps.setString(1, (String) row.get("code_module"));
                    ps.setString(2, (String) row.get("code_presentation"));
    				ps.setString(3, (String) row.get("module_presentation_length"));
    			} catch (SQLException e) {
    				throw new RuntimeException(e);
    			}
        	};
        	final String QUERY_INSERT_COURSES_LANDING = String.format("INSERT INTO courses (code_module, code_presentation, module_presentation_length) VALUES (?, ?, ?);");
        	batchInsert(QUERY_INSERT_COURSES_LANDING, mockLandingCourses, queryMapping);
      	        	
        	
        	///Landing to Gold
        	//format courses
//        	UnaryOperator<Map<String, Object>> formatCourses = (row)->{
//        		row.put("code_module",(String) row.get("code_module"));
//        		row.put("code_presentation",(String) row.get("code_presentation"));
//        		row.put("module_presentation_length",(int) row.get("module_presentation_length"));// todo format
//        		return row;
//        	};
//        	String landingCoursesStmt = String.format("select * from courses order by code_module limit 3;");
//
//        	List<Map<String, Object>> landingGoldCourses = this.mockJdbcTemplate.queryForList(landingCoursesStmt,landingDelta.get("courses")).stream().map(formatCourses).collect(Collectors.toList());
//        	
//        	
//        	log.info("Querying Test :: landingdb");
//        	String landingStmt = String.format("select * from courses order by code_module limit 3;");
//        	List<String> landingResult = this.landingJdbcTemplate.queryForList(landingStmt)
//        							.stream().map((m) -> m.values().toString()).collect(Collectors.toList());
//        	landingResult.forEach(x->log.info(x));
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
        
        log.info("Done cron");
    }
    
    public int batchInsert(String query, List incominglist, BiConsumer queryMapping) {
    	
    	 try {
         	final int batchSize = 500;
         	for (int index = 0; index < incominglist.size(); index += batchSize) {
                 final List<Map<String, Object>> batchList = incominglist.subList(index, Math.min(index + batchSize, incominglist.size()));
                 int[] inserted = getLandingJdbcTemplate().batchUpdate(query,
                     new BatchPreparedStatementSetter() {
                         @Override
                         public void setValues(PreparedStatement ps, int i)
                                 throws SQLException {
                         	Map<String, Object> row = batchList.get(i);
                         	queryMapping.accept(row, ps);
                         }

                         @Override
                         public int getBatchSize() {
                             return batchList.size();
                         }
                     });
                 log.info("Querying :: inserted :: " + inserted.length);

             }
         } catch(Exception e) {
         	throw new RuntimeException(e);
         }
    	return 0;
    }
    
    public Map<String,Object> formatDeltaMap(List<Map<String, Object>> map) {
    	UnaryOperator<Map<String, Object>> formatDelta = (row)->{
    		log.info("Mapping :: table_name :: " + row.get("table_name"));
    		row.put("table_name",(String) row.get("table_name"));
    		row.put("time_executed", (Timestamp) row.get("time_executed"));
    		return row;
    	};
    	log.info("formatDeltaMap :: size :: " + map.size());
    	return map.stream()
    			.map(formatDelta)
    			.collect(Collectors.toMap(m->(String)m.get("table_name")
    					,m->(Timestamp)m.get("time_executed")));
    }
}