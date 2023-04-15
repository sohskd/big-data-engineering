package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.GoldDAO;
import com.big.data.engineering3.dao.LandingDAO;
import com.big.data.engineering3.dao.MockDAO;
import com.big.data.engineering3.dao.SensitiveDAO;
import com.big.data.engineering3.dao.WorkDAO;
import com.big.data.engineering3.service.BatchJobService;
import com.big.data.engineering3.service.SparkJobService;
import com.big.data.engineering3.spark.GoldSparkConfig;
import com.big.data.engineering3.spark.LandingSparkConfig;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
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
public class SparkJobServiceImpl implements SparkJobService {
	
	@Autowired
    private SparkSession sparkSession;
    @Autowired
    private LandingSparkConfig landingSparkConfig;
    @Autowired
    private GoldSparkConfig goldSparkConfig;


    public void ingest_studentAssessment(List<String> errorList) {

    	try {
        	StructType studentAssessmentSchema = new StructType()
        			.add("id_assessment", DataTypes.IntegerType, false)
        			.add("id_student", DataTypes.IntegerType, false)
        			.add("date_submitted", DataTypes.IntegerType, false)
        			.add("is_banked", DataTypes.IntegerType, false)
        			.add("score", DataTypes.FloatType, false);
        	Dataset<Row> studentAssessmentDataSet = sparkSession.read()
        			.option("header", "true")
        			.schema(studentAssessmentSchema)
        			.csv(SQLConstants.LANDING_PATH_STUDENT_ASSESSMENT);
        	studentAssessmentDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_STUDENT_ASSESSMENT, goldSparkConfig.goldConnectionProperties());
        	cleanFiles(SQLConstants.LANDING_PATH_ROOT, SQLConstants.CODE_STUDENT_ASSESSMENT); 
    	} catch (Exception e) {
    		errorList.add("Error studentAssessment");
    		throw e;
    	}
    }
    
    public void ingest_studentInfo(List<String> errorList) {

    	try {
        	StructType studentInfoSchema = new StructType()
        			.add("code_module", DataTypes.StringType, false)
        			.add("code_presentation", DataTypes.StringType, false)
        			.add("id_student", DataTypes.IntegerType, false)
        			.add("gender", DataTypes.StringType, false)
        			.add("region", DataTypes.StringType, false)
        			.add("highest_education", DataTypes.StringType, false)
        			.add("imd_band", DataTypes.StringType, false)
        			.add("age_band", DataTypes.StringType, false)
        			.add("num_of_prev_attempts", DataTypes.IntegerType, false)
        			.add("studied_credits", DataTypes.IntegerType, false)
        			.add("disability", DataTypes.StringType, false)
        			.add("final_result", DataTypes.StringType, false);
        	Dataset<Row> studentInfoDataSet = sparkSession.read()
        			.option("header", "true")
        			.schema(studentInfoSchema)
        			.csv(SQLConstants.LANDING_PATH_STUDENT_INFO);
        	studentInfoDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_STUDENT_INFO, goldSparkConfig.goldConnectionProperties());
        	cleanFiles(SQLConstants.LANDING_PATH_ROOT, SQLConstants.CODE_STUDENT_INFO); 

    	} catch (Exception e) {
    		errorList.add("Error studentInfo");
    		throw e;
    	}
    }
    
    public void ingest_studentRegistration(List<String> errorList) {

    	try {
        	StructType studentRegistrationSchema = new StructType()
        			.add("code_module", DataTypes.StringType, false)
        			.add("code_presentation", DataTypes.StringType, false)
        			.add("id_student", DataTypes.IntegerType, false)
        			.add("date_registration", DataTypes.IntegerType, false)
        			.add("date_unregistration", DataTypes.IntegerType, false);
        	Dataset<Row> studentRegistrationDataSet = sparkSession.read()
        			.option("header", "true")
        			.schema(studentRegistrationSchema)
        			.csv(SQLConstants.LANDING_PATH_STUDENT_REGISTRATION);
        	studentRegistrationDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_STUDENT_REGISTRATION, goldSparkConfig.goldConnectionProperties());
        	cleanFiles(SQLConstants.LANDING_PATH_ROOT, SQLConstants.CODE_STUDENT_ASSESSMENT); 
    	} catch (Exception e) {
    		errorList.add("Error studentRegistration");
    		throw e;
    	}
    }
    
    public void ingest_studentVle(List<String> errorList) {

    	try {
        	StructType studentVleSchema = new StructType()
        			.add("code_module", DataTypes.StringType, false)
        			.add("code_presentation", DataTypes.StringType, false)
        			.add("id_student", DataTypes.IntegerType, false)
        			.add("id_site", DataTypes.IntegerType, false)
        			.add("date", DataTypes.IntegerType, false)
        			.add("sum_click", DataTypes.IntegerType, false);
        	Dataset<Row> studentVleDataSet = sparkSession.read()
        			.option("header", "true")
        			.schema(studentVleSchema)
        			.csv(SQLConstants.LANDING_PATH_STUDENT_VLE);
        	studentVleDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_STUDENT_VLE, goldSparkConfig.goldConnectionProperties());
        	cleanFiles(SQLConstants.LANDING_PATH_ROOT, SQLConstants.CODE_STUDENT_VLE); 
    	} catch (Exception e) {
    		errorList.add("Error studentVle");
    		throw e;
    	}
    }
    
    public void ingest_vle(List<String> errorList) {

    	try {
        	StructType vleSchema = new StructType()
        			.add("id_site", DataTypes.IntegerType, false)
        			.add("code_module", DataTypes.StringType, false)
        			.add("code_presentation", DataTypes.StringType, false)
        			.add("activity_type", DataTypes.StringType, false)
        			.add("week_from", DataTypes.IntegerType, false)
        			.add("week_to", DataTypes.IntegerType, false);
        	Dataset<Row> vleDataSet = sparkSession.read()
        			.option("header", "true")
        			.schema(vleSchema)
        			.csv("../big-data-engineering/data/downloaded/landing_csv/7_vle_*.csv");
        	vleDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_VLE, goldSparkConfig.goldConnectionProperties());
        	cleanFiles(SQLConstants.LANDING_PATH_ROOT, SQLConstants.CODE_VLE); 
    	} catch (Exception e) {
    		errorList.add("Error vle");
    		throw e;
    	}
    }
    public void cleanFiles(String dir, String fileCode) {
    	for (File file : new java.io.File(dir).listFiles()) {
    		if(file.getName().indexOf(fileCode)==0) {
        		log.info(file.getName()); 
        		file.delete();
    		}
    	}
    }
}