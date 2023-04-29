package com.big.data.engineering3.spark;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.dao.GoldDAO;
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
	@Autowired
	GoldDAO goldDAO;

	public Map<String, Timestamp> formatDeltaMap(List<Map<String, Object>> map) {
		UnaryOperator<Map<String, Object>> formatDelta = (row) -> {
			log.info("Mapping :: table_name :: " + row.get("table_name"));
			row.put("table_name", (String) row.get("table_name"));
			row.put("time_executed", (Timestamp) row.get("time_executed"));
			return row;
		};
		log.info("formatDeltaMap :: size :: " + map.size());
		return map.stream().map(formatDelta)
				.collect(Collectors.toMap(m -> (String) m.get("table_name"), m -> (Timestamp) m.get("time_executed")));
	}

	@RequestMapping("read-gold-test")
	public ResponseEntity<String> processSqlLandingZoneToGoldZone() {
		log.info("Running processLandingZoneToGoldZone");
		try {

			log.info("Querying :: Last Import from goldDB Delta");
			Map<String, Timestamp> goldDelta = formatDeltaMap(goldDAO.getDelta());
			// Assessments
			log.info("Querying :: Delta assessments from landingDB");
	
			Dataset<Row> landingCoursesDeltaInsert = landingSparkConfig.readSession(SQLConstants.TABLE_COURSES)
					.select("code_module","code_presentation","module_presentation_length")
					.filter("inserttimestamp" + ">'" + goldDelta.get(SQLConstants.TABLE_COURSES)+"'");
			landingCoursesDeltaInsert = landingCoursesDeltaInsert
					.withColumn("module_presentation_lengthTemp", landingCoursesDeltaInsert.col("module_presentation_length").cast(DataTypes.IntegerType))
					.drop(landingCoursesDeltaInsert.col("module_presentation_length"))
					.withColumnRenamed("module_presentation_lengthTemp", "module_presentation_length");

			landingCoursesDeltaInsert.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_COURSES, goldSparkConfig.goldConnectionProperties());
			return ResponseEntity.ok(responseTest(landingCoursesDeltaInsert));

//	        	
//	        	int insertedGoldAssessments = goldDAO.insertAssessments(landingAssessmentsDeltaInsert);
//	        	log.info("insertedGoldAssessments :: " + insertedGoldAssessments);
//	        	int updateGoldDeltaForAssessments = goldDAO.updateDelta(SQLConstants.TABLE_ASSESSMENTS);
//	        	
//	        	List<Map<String, Object>> landingAssessmentsDeltaUpdate = landingDAO.getAssessmentsByCHANGETIMESTAMP(goldDelta.get(SQLConstants.TABLE_ASSESSMENTS));
//	        	landingAssessmentsDeltaUpdate.forEach(m->log.info(m.values().toString()));
//	        	int updatedGoldAssessments = goldDAO.updateAssessments(landingAssessmentsDeltaUpdate);
//	        	log.info("updatedGoldAssessments :: " + updatedGoldAssessments);
//	        	
//	        	log.info("updateGoldDeltaForAssessments :: " + updateGoldDeltaForAssessments);// Note: delta time is delayed 
//	        	
//	        	
//	        	//Courses
//	        	log.info("Querying :: Delta courses from landingDB");
//	        	
//	        	List<Map<String, Object>> landingCoursesDeltaInsert = landingDAO.getCourseByINSERTTIMESTAMP(goldDelta.get(SQLConstants.TABLE_COURSES));
//	        	landingCoursesDeltaInsert.forEach(m->log.info(m.values().toString()));
//	        	int insertedGoldCourses = goldDAO.insertCourses(landingCoursesDeltaInsert);
//	        	log.info("insertedGoldCourses :: " + insertedGoldCourses);
//	        	
//	          	List<Map<String, Object>> landingCoursesDeltaUpdate = landingDAO.getCourseByCHANGETIMESTAMP(goldDelta.get(SQLConstants.TABLE_COURSES));
//	        	landingCoursesDeltaUpdate.forEach(m->log.info(m.values().toString()));
//	        	int updatedGoldCourses = goldDAO.updateCourses(landingCoursesDeltaUpdate);
//	        	log.info("updatedGoldCourses :: " + updatedGoldCourses);
//	        	
//	        	int updateGoldDeltaForCourses = goldDAO.updateDelta(SQLConstants.TABLE_COURSES);
//	        	
//	  
//	        	
//	        	
//	        	log.info("updateGoldDeltaForCourses :: " + updateGoldDeltaForCourses);// Note: delta time is delayed 

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@RequestMapping("read-csv")
	public ResponseEntity<String> getRowCount() {
		Dataset<Row> dataset = sparkSession.read().option("header", "true")
				.csv("../big-data-engineering/src/main/resources/raw_data.csv");
		return ResponseEntity.ok(responseTest(dataset));
	}

	@RequestMapping("gcp-test")
	public ResponseEntity<String> getMockGCP() {
		Dataset<Row> dataset = goldSparkConfig.readSession(SQLConstants.TABLE_DELTA);
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
		String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot")
				+ String.format("<h2>%s</h2>", "Spark version = " + sparkSession.sparkContext().version())
				+ String.format("<h3>%s</h3>", "Read csv..")
				+ String.format("<h4>Total records %d</h4>", dataset.count())
				+ String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString())
				+ dataset.showString(20, 20, true);
		return html;
	}

}
