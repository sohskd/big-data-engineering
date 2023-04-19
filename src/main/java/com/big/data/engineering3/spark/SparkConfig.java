package com.big.data.engineering3.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@PropertySource("classpath:application.properties")
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;
    
    @Value("${hdfs.service.account}")
    private String serviceAccount;

    @Value("${hdfs.project.id}")
    private String projectId;
    
    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        SparkContext context = javaSparkContext().sc();
    	org.apache.hadoop.conf.Configuration hdp = context.hadoopConfiguration();
    	hdp.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    	hdp.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    	hdp.set("fs.gs.project.id",projectId);
    	hdp.set("google.cloud.auth.service.account.enable", "true");
    	hdp.set("google.cloud.auth.service.account.json.keyfile", System.getProperty("user.dir")+serviceAccount);
    	
        return SparkSession
                .builder()
                .sparkContext(context)
                .appName("EBD2023")
//                .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.3.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.12")
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}