package com.spikeify.aerospikeql;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.spikeify.aerospikeql.execute.ResultsList;
import com.spikeify.aerospikeql.execute.ResultsMap;
import com.spikeify.aerospikeql.execute.ResultsType;

public interface Query<T> {

	Query setFilters(Filter[] filters);

	Query setQueryPolicy(QueryPolicy queryPolicy);

	Query setCurrentTimeMillis(Long currentTimeMillis);

	Query setCondition(String condition);

	ResultsMap asMap();

	ResultsList asList();

	ResultsType<T> asType(Class<T> clazz);
}
