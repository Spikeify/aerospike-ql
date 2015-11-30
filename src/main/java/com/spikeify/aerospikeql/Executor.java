package com.spikeify.aerospikeql;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.spikeify.aerospikeql.execute.Diagnostics;

import java.util.List;

public interface Executor<T> {

	Executor<T> setFilters(Filter[] filters);

	Executor<T> setPolicy(QueryPolicy queryPolicy);

	Executor<T> setCurrentTime(Long currentTimeMillis);

	Executor<T> setCondition(String condition);

	Diagnostics getDiagnostics();

	List<T> now();

}
