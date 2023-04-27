package com.big.data.engineering3.service.impl;

import com.big.data.engineering3.constant.SQLConstants;
import com.big.data.engineering3.service.SparkJobService;
import com.big.data.engineering3.spark.GoldSparkConfig;
import com.big.data.engineering3.spark.LandingSparkConfig;
import com.big.data.engineering3.spark.SparkConfig;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@Service
public class SparkJobServiceImpl implements SparkJobService {
	
	@Autowired
    private SparkSession sparkSession;
    @Autowired
    private LandingSparkConfig landingSparkConfig;
    @Autowired
    private GoldSparkConfig goldSparkConfig;
    @Autowired
    private SparkConfig sparkConfig;


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
        	markAsProcessed(goldSparkConfig.projectId, SQLConstants.LANDING_BUCKET,SQLConstants.LANDING_RAW_FOLDER,SQLConstants.LANDING_PROCESSED_FOLDER, SQLConstants.CODE_STUDENT_ASSESSMENT); 
    	} catch (Exception e) {
    		errorList.add("Error studentAssessment");
    		log.error(e.getMessage());
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
        	markAsProcessed(goldSparkConfig.projectId, SQLConstants.LANDING_BUCKET,SQLConstants.LANDING_RAW_FOLDER,SQLConstants.LANDING_PROCESSED_FOLDER, SQLConstants.CODE_STUDENT_INFO); 

    	} catch (Exception e) {
    		errorList.add("Error studentInfo");
    		log.error(e.getMessage());
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
        	markAsProcessed(goldSparkConfig.projectId, SQLConstants.LANDING_BUCKET,SQLConstants.LANDING_RAW_FOLDER,SQLConstants.LANDING_PROCESSED_FOLDER, SQLConstants.CODE_STUDENT_REGISTRATION); 
    	} catch (Exception e) {
    		errorList.add("Error studentRegistration");
    		log.error(e.getMessage());
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
        	markAsProcessed(goldSparkConfig.projectId, SQLConstants.LANDING_BUCKET,SQLConstants.LANDING_RAW_FOLDER,SQLConstants.LANDING_PROCESSED_FOLDER, SQLConstants.CODE_STUDENT_VLE); 
    	} catch (Exception e) {
    		errorList.add("Error studentVle");
    		log.error(e.getMessage());
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
        	
        	Dataset<Row> vleDataSet = sparkSession
        			.read()     			
        			.option("header", "true")
        			.schema(vleSchema)
        			.csv(SQLConstants.LANDING_PATH_VLE);
        	vleDataSet.write().mode(SaveMode.Append).jdbc(goldSparkConfig.url, SQLConstants.TABLE_VLE, goldSparkConfig.goldConnectionProperties());
        	markAsProcessed(goldSparkConfig.projectId, SQLConstants.LANDING_BUCKET,SQLConstants.LANDING_RAW_FOLDER,SQLConstants.LANDING_PROCESSED_FOLDER, SQLConstants.CODE_VLE); 

    	} catch (Exception e) {
    		errorList.add("Error vle");
    		log.error(e.getMessage());
    	}
    }
    public void markAsProcessed(String projectId,String bucket, String rawFolder, String processedFolder, String fileCode) throws IOException {
    	Storage storage = StorageOptions.getDefaultInstance().getService();
        
        // The name of the GCS bucket
        String bucketName = bucket;
        // The regular expression for matching blobs in the GCS bucket.
        // Example: '.*abc.*'
        String matchExpr = rawFolder+"/"+fileCode;

        List<String> results = listBlobs(storage, bucketName, Pattern.compile(matchExpr), rawFolder+"/");
        log.info("List Blob Results: " + results.size() + " items.");
        for (String result : results) {
        	log.info("Blob: " + result);
        	moveObject(projectId,bucket,result,bucket,result.replaceAll(rawFolder, processedFolder));
        }
    }
    
    
    // Lists all blobs in the bucket matching the expression.
    // Specify a regex here. Example: '.*abc.*'
    private static List<String> listBlobs(Storage storage, String bucketName, Pattern matchPattern, String directoryPrefix)
        throws IOException {
      List<String> results = new ArrayList<>();
  
      // Only list blobs in the current directory
      // (otherwise you also get results from the sub-directories).
      BlobListOption rootDirectory = BlobListOption.currentDirectory();
      BlobListOption directoryPrefixOpt = Storage.BlobListOption.prefix(directoryPrefix);
      Page<Blob> blobs = storage.list(bucketName,directoryPrefixOpt, rootDirectory);
      for (Blob blob : blobs.iterateAll()) {
        if (!blob.isDirectory() && matchPattern.matcher(blob.getName()).matches()) {
          results.add(blob.getName());
        }
      }
      return results;
    }
    public void moveObject(
    	      String projectId,
    	      String sourceBucketName,
    	      String sourceObjectName,
    	      String targetBucketName,
    	      String targetObjectName) {
    	    // The ID of your GCP project
    	    // String projectId = "your-project-id";

    	    // The ID of your GCS bucket
    	    // String bucketName = "your-unique-bucket-name";

    	    // The ID of your GCS object
    	    // String sourceObjectName = "your-object-name";

    	    // The ID of the bucket to move the object objectName to
    	    // String targetBucketName = "target-object-bucket"

    	    // The ID of your GCS object
    	    // String targetObjectName = "your-new-object-name";
    		Credentials credentials = null;
			try {
				credentials = GoogleCredentials.fromStream(new FileInputStream(System.getProperty("user.dir")+sparkConfig.serviceAccount));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	    Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
    	    BlobId source = BlobId.of(sourceBucketName, sourceObjectName);
    	    BlobId target = BlobId.of(targetBucketName, targetObjectName);

    	    // Optional: set a generation-match precondition to avoid potential race
    	    // conditions and data corruptions. The request returns a 412 error if the
    	    // preconditions are not met.
    	    Storage.BlobTargetOption precondition;
    	    if (storage.get(targetBucketName, targetObjectName) == null) {
    	      // For a target object that does not yet exist, set the DoesNotExist precondition.
    	      // This will cause the request to fail if the object is created before the request runs.
    	      precondition = Storage.BlobTargetOption.doesNotExist();
    	    } else {
    	      // If the destination already exists in your bucket, instead set a generation-match
    	      // precondition. This will cause the request to fail if the existing object's generation
    	      // changes before the request runs.
    	      precondition =
    	          Storage.BlobTargetOption.generationMatch(
    	              storage.get(targetBucketName, targetObjectName).getGeneration());
    	    }

    	    // Copy source object to target object
    	    storage.copy(
    	        Storage.CopyRequest.newBuilder().setSource(source).setTarget(target, precondition).build());
    	    Blob copiedObject = storage.get(target);
    	    // Delete the original blob now that we've copied to where we want it, finishing the "move"
    	    // operation
    	    storage.get(source).delete();

    	    System.out.println(
    	        "Moved object "
    	            + sourceObjectName
    	            + " from bucket "
    	            + sourceBucketName
    	            + " to "
    	            + targetObjectName
    	            + " in bucket "
    	            + copiedObject.getBucket());
    	  }
}