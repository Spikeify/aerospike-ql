package com.spikeify.aerospikeql;

import com.spikeify.Spikeify;

import java.util.Map;

/**
 * This is a helper service that provides an AerospikeQL instance.
 */
public class AerospikeQlService {

	private final Spikeify sfy;
	private final QueryUtils queryUtils;

	public AerospikeQlService(Spikeify sfy,
	                          String udfFolder) {
		this.sfy = sfy;
		this.queryUtils = new QueryUtils(sfy, udfFolder);
	}

	public AerospikeQlService(Spikeify sfy) {
		this.sfy = sfy;
		this.queryUtils = new QueryUtils(sfy, null);
	}

	/**
	 * Execute adhoc query and retrieve entities in a Map<String, Object>
	 *
	 * @param query - sql query
	 * @return An ExecutorAdhoc for adhoc queries
	 */
	public Executor<Map<String, Object>> execAdhoc(String query) {
		return new ExecutorAdhoc<>(sfy, queryUtils, null, query);
	}

	/**
	 * Execute adhoc query and map entities to their type.
	 * Note that this is supported only for select * queries.
	 *
	 * @param tClass - entity class
	 * @param query - sql query
	 * @param <T> - entity type
	 * @return An ExecutorAdhoc for adhoc queries
	 */
	public <T> Executor<T> execAdhoc(Class<T> tClass, String query) {
		return new ExecutorAdhoc<>(sfy, queryUtils, tClass, query);
	}

	/**
	 * Execute static query and retrieve entities in a Map<String, Object>
	 *
	 * @param query - sql query
	 * @param queryName - name of the query to execute
	 * @return An ExecutorStatic for static queries
	 */
	public Executor<Map<String, Object>> execStatic(String query, String queryName) {
		return new ExecutorStatic<>(sfy, queryUtils, null, query, queryName);
	}

	/**
	 * Execute static query and map entities to their type.
	 * Note that this is supported only for select * queries.
	 *
	 * @param tClass - entity class
	 * @param query - sql query
	 * @param queryName - name of the query to execute
	 * @param <T> - entity type
	 * @return An ExecutorStatic for static queries
	 */
	public <T> Executor<T> execStatic(Class<T> tClass, String query, String queryName) {
		return new ExecutorStatic<>(sfy, queryUtils, tClass, query, queryName);
	}




}
