package com.spikeify.aerospikeql;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.spikeify.aerospikeql.execute.Profile;

import java.util.List;

/**
 * This is the main class of AerospikeQL. All command chains start from this class.
 */
public interface Executor<T> {

	/**
	 * Set Aerospike's filters on secondary indices
	 *
	 * @param filters - set Aerospike's filters
	 * @return Executor
	 */
	Executor<T> setFilters(Filter[] filters);

	/**
	 * Set Aerospike's query policy
	 *
	 * @param queryPolicy - Aerospike's query policy
	 * @return Executor
	 */
	Executor<T> setPolicy(QueryPolicy queryPolicy);

	/**
	 * This helper method set current time and it is intended for unit testing.
	 *
	 * @param currentTimeMillis - current time in milliseconds
	 * @return Executor
	 */
	Executor<T> setCurrentTime(Long currentTimeMillis);

	/**
	 * Adhoc condition injection for static queries
	 *
	 * @param condition - adhoc condition to inject
	 * @return Executor
	 */
	Executor<T> setCondition(String condition);


	/**
	 * transform query to match fields in Entity (longer bin names, primary key etc) but return results in a Map<String, Object>
	 *
	 * @return Executor
	 */
	<E>Executor<T> mapQuery(Class<E> clazz);

	/**
	 * Get information about of query execution
	 *
	 * @return Profile class
	 */
	Profile getProfile();

	/**
	 * Retrieve results of a query
	 *
	 * @return List<T> with Objects or null in case of some error
	 */
	List<T> now();

}
