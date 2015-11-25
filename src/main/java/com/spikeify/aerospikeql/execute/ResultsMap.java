package com.spikeify.aerospikeql.execute;

import java.util.List;
import java.util.Map;

/**
 * Created by roman on 17/08/15.

 * Data structure for Result set and query diagnostics
 */
public class ResultsMap {

	private final List<Map<String, Object>> resultsData;
	private final QueryDiagnostics queryDiagnostics;

	public ResultsMap(List<Map<String, Object>> resultsData, QueryDiagnostics queryDiagnostics) {
		this.resultsData = resultsData;
		this.queryDiagnostics = queryDiagnostics;
	}

	public List<Map<String, Object>> getResultsData() {
		return resultsData;
	}

	public QueryDiagnostics getQueryDiagnostics() {
		return queryDiagnostics;
	}
}
