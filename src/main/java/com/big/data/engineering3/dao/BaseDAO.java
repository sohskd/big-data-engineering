package com.big.data.engineering3.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import com.big.data.engineering3.service.impl.BatchJobServiceImpl;

import lombok.extern.slf4j.Slf4j;


public interface BaseDAO {
    public static int batchInsert(JdbcTemplate jdbcTemplate,String query, List incominglist, BiConsumer queryMapping) {
    int total = 0;	
   	 try {
        	final int batchSize = 100;
        	for (int index = 0; index < incominglist.size(); index += batchSize) {
                final List<Map<String, Object>> batchList = incominglist.subList(index, Math.min(index + batchSize, incominglist.size()));
                int[] inserted = jdbcTemplate.batchUpdate(query,
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
                total += inserted.length;
        	}
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
   	return total;
   }
  
}
