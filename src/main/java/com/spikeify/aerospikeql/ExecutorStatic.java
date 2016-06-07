package com.spikeify.aerospikeql;

import com.aerospike.client.Value;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.execute.Retrieve;

import java.util.List;
import java.util.Map;

class ExecutorStatic<T> extends ExecutorAdhoc<T> implements Executor<T> {
	private final String queryName;

	public ExecutorStatic(Spikeify sfy,
	                      QueryUtils queryUtils,
	                      Class<T> tClass,
	                      String query,
	                      String queryName) {
		super(sfy, queryUtils, tClass, query);
		this.queryName = queryName;
	}


	@Override
	protected List<Map<String, Object>> execQuery() {
		if (query != null && queryName != null) {
			// Execute aggregation query with LUA
			if (currentTimeMillis == null) {
				currentTimeMillis = System.currentTimeMillis(); // used for now() function in select, having and measuring query execution time
			}

			queryFields = queryUtils.getQueryFiels(query);
			if (queryFields != null) {
				Statement statement = new Statement();
				statement.setNamespace(queryFields.getNamespace());
				statement.setSetName(queryFields.getSet());

				//secondary index
				if (filters != null) {
					statement.setFilters(filters);
				}

				String conditionInjection = "";
				if (condition != null) {
					conditionInjection = new ConditionProcessor().process(condition);
				}

				ResultSet rs = sfy.getClient().queryAggregate(queryPolicy, statement, queryName, "main", Value.get(currentTimeMillis), Value.get(conditionInjection)); //pass parameters to lua script
				Retrieve retrieveResults = new Retrieve(queryFields, rs, currentTimeMillis);
				List<Map<String, Object>> resultList = retrieveResults.retrieve();
				profile = retrieveResults.getProfile();
				return resultList;
			}
		}
		return null;

	}


}
