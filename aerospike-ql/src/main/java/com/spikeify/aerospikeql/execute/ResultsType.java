package com.spikeify.aerospikeql.execute;

import java.util.List;

public class ResultsType<T> {

	private final List<T> resultsData;
	private final QueryDiagnostics queryDiagnostics;

	public ResultsType(List<T> resultsData, QueryDiagnostics queryDiagnostics) {
		this.resultsData = resultsData;
		this.queryDiagnostics = queryDiagnostics;
	}

	public List<T> getResultsData() {
		return resultsData;
	}

	public QueryDiagnostics getQueryDiagnostics() {
		return queryDiagnostics;
	}
}
