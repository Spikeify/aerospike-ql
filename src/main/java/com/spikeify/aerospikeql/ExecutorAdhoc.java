package com.spikeify.aerospikeql;

import com.aerospike.client.Value;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.spikeify.ClassMapper;
import com.spikeify.FieldMapper;
import com.spikeify.MapperUtils;
import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.execute.Profile;
import com.spikeify.aerospikeql.execute.Retrieve;
import com.spikeify.aerospikeql.parse.QueryFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class ExecutorAdhoc<T> implements Executor<T> {

	private static final Logger log = LoggerFactory.getLogger(ExecutorAdhoc.class);
	final Spikeify sfy;
	final QueryUtils queryUtils;
	QueryFields queryFields;
	Profile profile;
	private final Class<T> tClass;
	String query;
	String condition;
	Filter[] filters;
	QueryPolicy queryPolicy;
	Long currentTimeMillis;
	private EntityMetaData entityMetaData;
	private Class<?> mappingClass;


	public ExecutorAdhoc(Spikeify sfy,
	                     QueryUtils queryUtils,
	                     Class<T> tClass,
	                     String query) {
		this.sfy = sfy;
		this.queryUtils = queryUtils;
		this.tClass = tClass;
		this.query = query;
	}

	@Override
	public Executor<T> setFilters(Filter[] filters) {
		this.filters = filters;
		return this;
	}

	@Override
	public Executor<T> setPolicy(QueryPolicy queryPolicy) {
		this.queryPolicy = queryPolicy;
		return this;
	}

	/**
	 * This setter is only for unit testing
	 *
	 * @param currentTimeMillis current time in milliseconds
	 * @return ExecutorAdhoc
	 */
	public Executor<T> setCurrentTime(Long currentTimeMillis) {
		this.currentTimeMillis = currentTimeMillis;
		return this;
	}

	@Override
	public Executor<T> setCondition(String condition) {
		this.condition = condition;
		return this;
	}

	@Override
	public <E>Executor<T> mapQuery(Class<E> clazz) {
		mappingClass = clazz;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<T> now() {
		if (tClass != null && !isSelectAll(query)) {
			log.error("Typed queries can be only used with SELECT *.");
			return null;
		}

		if (tClass == null) {
			if(mappingClass != null){
				query = queryTransformation(mappingClass, query);
			}
			List<Map<String, Object>> resultList = execQuery();
			return (List<T>) resultList;

		} else {
			query = queryTransformation(tClass, query);
			ClassMapper<T> classMapper = new ClassMapper<>(tClass);

			List<Map<String, Object>> resultsList = execQuery();

			List<T> resultsListTyped = new ArrayList<>();
			if (resultsList != null) {
				while (resultsList.size() > 0) {
					Map<String, Object> entity = resultsList.remove(0);
					T instance = createNewInstance(tClass);
					if (instance == null) {
						log.error("class " + tClass.getName() + " should have default constructor and should be static if inner class.");
						return null;
					}

					fillInstance(classMapper, entity, instance);
					resultsListTyped.add(instance);
				}
				return resultsListTyped;
			}
		}
		return null;
	}

	private void fillInstance(ClassMapper<T> classMapper, Map<String, Object> entity, T instance) {
		if (entity.containsKey(entityMetaData.primaryKey)) {
			if (entityMetaData.isPrimaryKeyString()) {
				classMapper.setUserKey(instance, (String) entity.get(entityMetaData.getPrimaryKey()));
			} else {
				classMapper.setUserKey(instance, (Long) entity.get(entityMetaData.getPrimaryKey()));
			}
		}

		if (entityMetaData.getGenerationKey() != null && entityMetaData.getExpirationKey() != null && entity.containsKey(entityMetaData.getGenerationKey()) && entity.containsKey(entityMetaData.getExpirationKey())) {
			classMapper.setMetaFieldValues(instance, queryFields.getNamespace(), queryFields.getSet(), (int) entity.get(entityMetaData.getGenerationKey()), (int) entity.get(entityMetaData.getExpirationKey()));
		}
		classMapper.setFieldValues(instance, entity);

	}

	private T createNewInstance(Class<T> clazz) {
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

	List<Map<String, Object>> execQuery() {
		if (query != null) {
			// Execute aggregation query with LUA
			if (currentTimeMillis == null) {
				currentTimeMillis = System.currentTimeMillis(); // used for now() function in select, having and measuring query execution time
			}

			String queryName = UUID.randomUUID().toString();

			queryFields = queryUtils.addUdf(queryName, query);
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

				queryUtils.removeUdf(queryName);
				return resultList;
			}
		}
		return null;

	}

	String queryTransformation(final Class clazz, String query) {
		log.info("query before transformation: {}", query);

		Map<String, FieldMapper> fieldMappers = new HashMap<String, FieldMapper>() {{
			put(Definitions.primaryKey, MapperUtils.getUserKeyFieldMapper(clazz));
			put(Definitions.generation, MapperUtils.getGenerationFieldMapper(clazz));
			put(Definitions.expiration, MapperUtils.getExpirationFieldMapper(clazz));
		}};


		String primaryKey = null;
		if (fieldMappers.get(Definitions.primaryKey) != null) {
			primaryKey = fieldMappers.get(Definitions.primaryKey).getFieldName();
		}

		String generationKey = null;
		if (fieldMappers.get(Definitions.generation) != null) {
			generationKey = fieldMappers.get(Definitions.generation).getFieldName();
		}

		String expirationKey = null;
		if (fieldMappers.get(Definitions.expiration) != null) {
			expirationKey = fieldMappers.get(Definitions.expiration).getFieldName();
		}

		entityMetaData = new EntityMetaData(primaryKey, generationKey, expirationKey);

		//process select *
		boolean selectAll = false;
		if (isSelectAll(query)) {
			selectAll = true;
			Map<String, String> binMappings = MapperUtils.getBinMappings(clazz);
			List<String> mappedFields = new ArrayList<>(binMappings.keySet());

			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null) {
					mappedFields.add(fieldMapper.getValue().getFieldName());
				}
			}
			Collections.sort(mappedFields);
			String bins = join(mappedFields, ", ");
			//process aerospike special fields
			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null) {
					bins = bins.replaceAll("\\b" + fieldMapper.getValue().getFieldName() + "\\b", fieldMapper.getKey() + "\\(\\)" + " as " + fieldMapper.getValue().getFieldName());
					if (fieldMapper.getValue().field.getType().getName().equals("java.lang.String")) {
						entityMetaData.setPrimaryKeyString(true);
					}

				}
			}

			query = query.replaceAll("(?i)select \\*", "SELECT " + bins);
		}

		Map<String, String> binMappings = MapperUtils.getBinMappings(clazz);
		for (Map.Entry<String, String> entry : binMappings.entrySet()) {
			if (!entry.getValue().equals(entry.getKey())) {
				query = query.replaceAll("\\b" + entry.getValue() + "\\b", entry.getKey());
			}
		}

		//process aerospike special fields
		if (!selectAll) {
			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null && query.contains(fieldMapper.getValue().getFieldName())) {
					query = query.replaceAll("\\b" + fieldMapper.getValue().getFieldName() + "\\b", fieldMapper.getKey() + "\\(\\)" + " as " + fieldMapper.getValue().getFieldName());
				}
			}
		}

		log.info("query after transformation: {}", query);
		return query;
	}

	private String join(List<String> fields, String delimiter) {
		StringBuilder output = new StringBuilder();
		for (String field : fields) {
			output.append(field).append(delimiter);
		}
		return output.substring(0, output.length() - delimiter.length());
	}

	private boolean isSelectAll(String query) {
		query = query.trim().replaceAll(" +", " ");
		return query.toLowerCase().contains("select *");
	}


	@Override
	public Profile getProfile() {
		return profile;
	}

	private class EntityMetaData {
		private final String primaryKey;
		private final String generationKey;
		private final String expirationKey;
		private boolean primaryKeyString;

		public EntityMetaData(String primaryKey, String generationKey, String expirationKey) {
			this.primaryKey = primaryKey;
			this.generationKey = generationKey;
			this.expirationKey = expirationKey;
		}

		public String getPrimaryKey() {
			return primaryKey;
		}

		public String getGenerationKey() {
			return generationKey;
		}

		public String getExpirationKey() {
			return expirationKey;
		}

		public boolean isPrimaryKeyString() {
			return primaryKeyString;
		}

		public void setPrimaryKeyString(boolean primaryKeyString) {
			this.primaryKeyString = primaryKeyString;
		}
	}
}
