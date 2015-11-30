package com.spikeify.aerospikeql;

import com.spikeify.Spikeify;

import java.util.Map;

public class AerospikeQlService {

	private final Spikeify sfy;
	private final QueryUtils queryUtils;

	public AerospikeQlService(Spikeify sfy,
	                          QueryUtils queryUtils) {
		this.sfy = sfy;
		this.queryUtils = queryUtils;
	}

	public Executor<Map<String,Object>> execAdhoc(String query) {
		return new ExecutorAdhoc<>(sfy, queryUtils, null, query);
	}

	public <T>Executor<T> execAdhoc(Class<T> tClass, String query) {
		return new ExecutorAdhoc<>(sfy, queryUtils, tClass, query);
	}

	public <T>Executor<T> execStatic(Class<T> tClass, String query, String queryName) {
		return new ExecutorStatic<>(sfy, queryUtils, tClass, query, queryName);
	}

	public Executor<Map<String,Object>> execStatic(String query, String queryName) {
		return new ExecutorStatic<>(sfy, queryUtils, null, query, queryName);
	}




}
