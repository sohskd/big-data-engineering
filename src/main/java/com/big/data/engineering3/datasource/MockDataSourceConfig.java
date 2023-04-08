package com.big.data.engineering3.datasource;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class MockDataSourceConfig {
		
    @Bean
    @ConfigurationProperties("spring.datasource.mock")
    public DataSourceProperties mockDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    public DataSource mockDataSource() {
        return mockDataSourceProperties()
          .initializeDataSourceBuilder()
          .build();
    }
    
    @Bean
    public JdbcTemplate mockJdbcTemplate(@Qualifier("mockDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}