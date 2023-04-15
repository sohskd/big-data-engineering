package com.big.data.engineering3.spark;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@PropertySource("classpath:application.properties")
public class GoldSparkConfig {

    @Value("${spring.datasource.gold.username}")
    private String user;

    @Value("${spring.datasource.gold.password}")
    private String password;

    @Value("${spring.datasource.gold.url}")
    public String url;
    
    @Autowired
    private SparkSession sparkSession;
    
    @Bean
    public Properties goldConnectionProperties() {
    	Properties connectionProperties = new Properties();
    	connectionProperties.put("user", user);
		connectionProperties.put("password",password);

        return connectionProperties;
    }
    
    public Dataset<Row> readSession(String table) {
    	 return sparkSession.read().jdbc(url, table, goldConnectionProperties());
    }
}