package com.spikeify.aerospikeql;

import com.aerospike.client.Value;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.execute.ResultsMap;
import com.spikeify.aerospikeql.execute.RetrieveResults;
import com.spikeify.aerospikeql.parse.QueryFields;

import java.util.UUID;

class QueryStatic extends QueryAdhoc implements Query {
	private String queryName;

	public QueryStatic(Spikeify sfy,
	                   QueryUtils queryUtils,
	                   String query,
	                   String queryName) {
		super(sfy, queryUtils, query);
		this.queryName = queryName;
	}


	@Override
	protected ResultsMap execQuery() {
		if (query != null) {

			if (queryName == null) {
				queryName = UUID.randomUUID().toString();
			}

			QueryFields queryFields = queryUtils.addUdf(queryName, query);
			if (queryFields != null) {
				Statement statement = new Statement();
				statement.setNamespace(queryFields.getNamespace());
				statement.setSetName(queryFields.getSet());

				//secondary index
				if (filters != null) {
					statement.setFilters(filters);
				}

				// Execute aggregation query with LUA
				if (currentTimeMillis == null) {
					currentTimeMillis = System.currentTimeMillis(); // used for now() function in select, having and measuring query execution time
				}

				String conditionInjection = "";
				if (condition != null) {
					conditionInjection = new ConditionProcessor().process(condition);
				}

				ResultSet rs = sfy.getClient().queryAggregate(queryPolicy, statement, queryName, "main", Value.get(currentTimeMillis), Value.get(conditionInjection)); //pass parameters to lua script
				RetrieveResults retrieveResults = new RetrieveResults(queryFields, rs, currentTimeMillis);
				return retrieveResults.retrieve();

			}
		}
		return null;

	}


}
