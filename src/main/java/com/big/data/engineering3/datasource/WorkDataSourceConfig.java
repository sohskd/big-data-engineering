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
public class WorkDataSourceConfig {
		
    @Bean
    @ConfigurationProperties("spring.datasource.work")
    public DataSourceProperties workDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    public DataSource workDataSource() {
        return workDataSourceProperties()
          .initializeDataSourceBuilder()
          .build();
    }
    
    @Bean
    public JdbcTemplate workJdbcTemplate(@Qualifier("workDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}