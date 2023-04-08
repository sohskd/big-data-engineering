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
public class SensitiveDataSourceConfig {
		
    @Bean
    @ConfigurationProperties("spring.datasource.sensitive")
    public DataSourceProperties sensitiveDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    public DataSource sensitiveDataSource() {
        return sensitiveDataSourceProperties()
          .initializeDataSourceBuilder()
          .build();
    }
    
    @Bean
    public JdbcTemplate sensitiveJdbcTemplate(@Qualifier("sensitiveDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}