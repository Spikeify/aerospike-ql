package com.spikeify.aerospikeql.execute;

import java.util.List;

public class ResultsList {

	private final List<List<Object>> resultsData;
	private final QueryDiagnostics queryDiagnostics;

	public ResultsList(List<List<Object>> resultsData, QueryDiagnostics queryDiagnostics) {
		this.resultsData = resultsData;
		this.queryDiagnostics = queryDiagnostics;
	}

	public List<List<Object>> getResultsData() {
		return resultsData;
	}

	public QueryDiagnostics getQueryDiagnostics() {
		return queryDiagnostics;
	}
}
