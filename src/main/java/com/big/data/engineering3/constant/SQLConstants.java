package com.big.data.engineering3.constant;

public class SQLConstants {
	
	//Tables
	public final static String TABLE_DELTA = "delta";
	public final static String TABLE_ASSESSMENTS = "assessments";
	public final static String TABLE_COURSES = "courses";
	public final static String TABLE_STUDENT_ASSESSMENT = "studentAssessment";
	public final static String TABLE_STUDENT_INFO = "studentInfo";
	public final static String TABLE_STUDENT_REGISTRATION = "studentRegistration";
	public final static String TABLE_STUDENT_VLE = "studentVle";
	public final static String TABLE_VLE = "vle";
//	public final static String TABLE_VLE = "vle_test";
	
	//Landing Paths
//	public final static String LANDING_PATH_ROOT = "../big-data-engineering/data/downloaded/landing_csv";
	public final static String LANDING_BUCKET = "zone_landing_ebd_2023";
	public final static String LANDING_RAW_FOLDER = "des_raw_csv";
	public final static String LANDING_PROCESSED_FOLDER = "processed_csv";
	public final static String LANDING_PATH_ROOT = "gs://"+LANDING_BUCKET+"/"+LANDING_RAW_FOLDER;
	public final static String LANDING_PATH_STUDENT_ASSESSMENT = LANDING_PATH_ROOT+"/"+"2_*";
	public final static String LANDING_PATH_STUDENT_INFO = LANDING_PATH_ROOT+"/"+"4_*";
	public final static String LANDING_PATH_STUDENT_REGISTRATION = LANDING_PATH_ROOT+"/"+"5_*";
	public final static String LANDING_PATH_STUDENT_VLE = LANDING_PATH_ROOT+"/"+"6_*";;
	public final static String LANDING_PATH_VLE = LANDING_PATH_ROOT+"/"+"7_*";
	
	public final static String CODE_STUDENT_ASSESSMENT = "2_.*";
	public final static String CODE_STUDENT_INFO = "4_.*";
	public final static String CODE_STUDENT_REGISTRATION = "5_.*";
	public final static String CODE_STUDENT_VLE = "6_.*";;
	public final static String CODE_VLE = "7_.*";
	
	public final static String QUERY_SELECT_DELTA = "SELECT table_name, time_executed FROM delta;";
	public final static String QUERY_UPDATE_DELTA = "UPDATE delta SET time_executed = now() WHERE table_name = ?;";
	
	//Assessments
	public final static String QUERY_SELECT_ASSESSMENTS_BY_INSERTTIMESTAMP = "SELECT code_module,code_presentation,id_assessment,assessment_type,date,weight FROM assessments where inserttimestamp > ?;";
	public final static String QUERY_SELECT_ASSESSMENTS_BY_CHANGETIMESTAMP = "SELECT code_module,code_presentation,id_assessment,assessment_type,date,weight FROM assessments where changetimestamp > ?;";
	public final static String QUERY_INSERT_ASSESSMENTS= "INSERT INTO assessments (code_module,code_presentation,assessment_type,date,weight,id_assessment) VALUES (?, ?, ?,?, ?, ?);";
	public final static String QUERY_UPDATE_ASSESSMENTS= "UPDATE assessments SET code_module = ?, code_presentation = ?, assessment_type= ?, date = ?, weight = ? where id_assessment = ?";

	//Courses
	public final static String QUERY_SELECT_COURSES_BY_INSERTTIMESTAMP = "SELECT code_module,code_presentation,module_presentation_length FROM courses where inserttimestamp > ?;";
	public final static String QUERY_SELECT_COURSES_BY_CHANGETIMESTAMP = "SELECT code_module,code_presentation,module_presentation_length FROM courses where changetimestamp > ?;";
	public final static String QUERY_INSERT_COURSES= "INSERT INTO courses (code_module, code_presentation, module_presentation_length) VALUES (?, ?, ?);";
	public final static String QUERY_UPDATE_COURSES= "UPDATE courses SET module_presentation_length  = ? where code_module = ? and code_presentation = ? ; ";


}
