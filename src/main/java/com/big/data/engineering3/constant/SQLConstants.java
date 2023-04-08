package com.big.data.engineering3.constant;

public class SQLConstants {
	
	public final static String TABLE_DELTA = "delta";
	public final static String TABLE_ASSESSMENTS = "assessments";
	public final static String TABLE_COURSES = "courses";
	
	public final static String QUERY_SELECT_DELTA = "SELECT table_name, time_executed FROM delta;";
	public final static String QUERY_UPDATE_DELTA = "UPDATE delta SET time_executed = now() WHERE table_name = ?;";
	//Assessments
	public final static String QUERY_SELECT_ASSESSMENTS_BY_DELTA = "SELECT code_module,code_presentation,id_assessment,assessment_type,date,weight FROM assessments where changetimestamp > ?;";
	public final static String QUERY_INSERT_ASSESSMENTS= "INSERT INTO assessments (code_module,code_presentation,assessment_type,date,weight,id_assessment) VALUES (?, ?, ?,?, ?, ?);";
	//Courses
	public final static String QUERY_SELECT_COURSES_BY_DELTA = "SELECT code_module,code_presentation,module_presentation_length FROM courses where changetimestamp > ?;";
	public final static String QUERY_INSERT_COURSES= "INSERT INTO courses (code_module, code_presentation, module_presentation_length) VALUES (?, ?, ?);";


}
