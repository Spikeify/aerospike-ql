package com.spikeify.aerospikeql;

import com.aerospike.client.Value;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.spikeify.ClassMapper;
import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.execute.ResultsList;
import com.spikeify.aerospikeql.execute.ResultsMap;
import com.spikeify.aerospikeql.execute.ResultsType;
import com.spikeify.aerospikeql.execute.RetrieveResults;
import com.spikeify.aerospikeql.parse.QueryFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class QueryAdhoc implements Query {

	private static final Logger log = LoggerFactory.getLogger(QueryAdhoc.class);

	protected final Spikeify sfy;
	protected final QueryUtils queryUtils;
	protected String query;
	protected String condition;
	protected Filter[] filters;
	protected QueryPolicy queryPolicy;
	protected Long currentTimeMillis;

	public QueryAdhoc(Spikeify sfy,
	                  QueryUtils queryUtils,
	                  String query) {
		this.sfy = sfy;
		this.queryUtils = queryUtils;
		this.query = query;
	}

	@Override
	public Query setFilters(Filter[] filters) {
		this.filters = filters;
		return this;
	}

	@Override
	public Query setQueryPolicy(QueryPolicy queryPolicy) {
		this.queryPolicy = queryPolicy;
		return this;
	}

	/**
	 * This setter is only for unit testing
	 *
	 * @param currentTimeMillis current time in milliseconds
	 * @return
	 */
	@Override
	public Query setCurrentTimeMillis(Long currentTimeMillis) {
		this.currentTimeMillis = currentTimeMillis;
		return this;
	}

	@Override
	public Query setCondition(String condition) {
		this.condition = condition;
		return this;
	}

	public ResultsList asList() {
		ResultsMap resultsMap = execQuery();
		List<List<Object>> resultsList = new ArrayList<>();
		if (resultsMap != null && resultsMap.getResultsData() != null) {
			while (resultsMap.getResultsData().size() > 0) {
				Map<String, Object> entry = resultsMap.getResultsData().remove(0);
				resultsList.add(new ArrayList<>(entry.values()));
			}
			return new ResultsList(resultsList, resultsMap.getQueryDiagnostics());
		}
		return null;
	}

	@Override
	public ResultsMap asMap() {
		return execQuery();
	}


	@Override
	public <T> ResultsType<T> asType(Class<T> clazz) {
		if (clazz != null) {
			query = queryUtils.queryTransformation(clazz, query);

			ResultsMap resultsMap = execQuery();
			List<T> resultsType = new ArrayList<>();

			if (resultsMap != null && resultsMap.getResultsData() != null) {
				while (resultsMap.getResultsData().size() > 0) {
					Map<String, Object> map = resultsMap.getResultsData().remove(0);
					T instance = createNewInstance(clazz);
					if (instance == null) {
						log.error("class " + clazz + " should have default constructor and should be static if inner class.");
						return null;
					}
					ClassMapper<T> classMapper = new ClassMapper<>(clazz);
					classMapper.setFieldValues(instance, map);
					resultsType.add(instance);
				}
				return new ResultsType<>(resultsType, resultsMap.getQueryDiagnostics());
			}
		}
		return null;
	}

	private <T> T createNewInstance(Class<T> clazz) {
		T instance;
		try {
			instance = clazz.newInstance();
		} catch (InstantiationException e) {
			log.error(e.getMessage());
			return null;
		} catch (IllegalAccessException e) {
			log.error(e.getMessage());
			return null;
		}
		return instance;
	}

	protected ResultsMap execQuery() {
		if (query != null) {
			String queryName = UUID.randomUUID().toString();

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
				ResultsMap resultsMap = RetrieveResults.retrieve(queryFields, rs, currentTimeMillis);

				queryUtils.removeUdf(queryName);
				return resultsMap;
			}
		}
		return null;

	}


}
