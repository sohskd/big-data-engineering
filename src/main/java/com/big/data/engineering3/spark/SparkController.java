package com.big.data.engineering3.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.service.SparkJobService;

import lombok.extern.slf4j.Slf4j;

@RequestMapping("spark-context")
@Controller
@Slf4j
public class SparkController {
    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private LandingSparkConfig landingSparkConfig;
    @Autowired
    private GoldSparkConfig goldSparkConfig;
    @Autowired
    private SparkJobService sparkJobService;
    
    @RequestMapping("read-csv")
    public ResponseEntity<String> getRowCount() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("../big-data-engineering/src/main/resources/raw_data.csv");
        return ResponseEntity.ok(responseTest(dataset));
    }
    
    @RequestMapping("gcp-test")
    public ResponseEntity<String> getMockGCP() {
    	Dataset<Row> dataset = goldSparkConfig.readSession(SQLConstants.TABLE_COURSES);
        return ResponseEntity.ok(responseTest(dataset));
    }
    
    @RequestMapping("ingest-landing-gold")
    public ResponseEntity<String> ingestLandingToGold() {
    	List<String> errorList = new ArrayList<String>();
//    	sparkJobService.ingest_studentAssessment(errorList);
//    	sparkJobService.ingest_studentInfo(errorList);
//    	sparkJobService.ingest_studentRegistration(errorList);
//    	sparkJobService.ingest_studentVle(errorList);
//    	sparkJobService.ingest_vle(errorList);
    	return ResponseEntity.ok(errorList.toString());
    }
    
    

    public String responseTest(Dataset<Row> dataset) {
    	String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
		        String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
		        String.format("<h3>%s</h3>", "Read csv..") +
		        String.format("<h4>Total records %d</h4>", dataset.count()) +
		        String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
		        dataset.showString(20, 20, true);
    	return html;
    }

}
