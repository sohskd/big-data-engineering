package com.big.data.engineering3.service;

import java.util.List;

public interface SparkJobService {

	void ingest_studentAssessment(List<String> errorList);
	void ingest_studentInfo(List<String> errorList);
	void ingest_studentRegistration(List<String> errorList);
	void ingest_studentVle(List<String>errorList);
	void ingest_vle(List<String> errorList);
}
