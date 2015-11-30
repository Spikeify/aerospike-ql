package com.spikeify.aerospikeql.execute;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.query.ResultSet;
import com.spikeify.aerospikeql.Definitions;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.HavingField;
import com.spikeify.aerospikeql.parse.fields.OrderField;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by roman on 17/08/15.
 *
 * Retrieve a List<Map<String, Object>> data structure (ResultsSet and QueryDiagnostics)
 */

public class Retrieve {

	private final QueryFields queryFields;
	private final ResultSet rs;
	private final long overallStart;
	private Profile profile;

	public Retrieve(QueryFields queryFields, ResultSet rs, long overallStart) {
		this.queryFields = queryFields;
		this.rs = rs;
		this.overallStart = overallStart;
	}

	public List<Map<String, Object>> retrieve() {
		boolean groupedResults = queryFields.getGroupList().size() > 0;
		boolean orderedResults = queryFields.getOrderFields().getOrderList().size() > 0;
		Map<String, Object> diagnostic = null;
		List<Map<String, Object>> resultList = new ArrayList<>();
		Set<String> distinctCounters = queryFields.getSelectField().getDistinctCounters();
		HavingField having = queryFields.getHavingField();

		//set having expression
		if (queryFields.getHavingField().getFields().size() > 0)
			having.setExpression(overallStart * 1000);

		try {
			while (rs.next()) {
				Object result = rs.getObject();

				if (groupedResults) { //all rows are in a single hash map
					diagnostic = aggregationResultsList(result, resultList, having, queryFields.getAverages(), distinctCounters);

				} else { //results come in separated hash maps. This are queries without group by statements
					diagnostic = basicResultsList(result, resultList, queryFields.getAverages(), distinctCounters);
					if (queryFields.getLimit() == resultList.size()) {
						break;
					}
				}
			}
		} catch (AerospikeException e) {
			e.printStackTrace();
		} finally {
			if (!(rs == null))
				rs.close();
		}

		if (orderedResults) {
			sortElements(resultList, queryFields.getOrderFields());
		}

		if (diagnostic == null) { //without group by statements
			diagnostic = new HashMap<>();
			diagnostic.put("count", resultList.size());
		}

		if (queryFields.getLimit() != -1) { //limit statements
			if (resultList.size() > queryFields.getLimit()) {
				resultList = resultList.subList(0, queryFields.getLimit());
			}
		}

		//remove fields that are not in select statements and set correct order to field.
		List<String> selectFields = queryFields.getSelectField().getSelectList();
		if (!Definitions.isSelectAll(selectFields)) {
			for (int i = 0; i < resultList.size(); i++) {
				resultList.set(i, convertMapToSortedMap(resultList.get(i), queryFields.getSelectField().getAliases()));
			}
		}

		long overallEnd = System.currentTimeMillis();
		long executionTime = overallEnd - overallStart;
		this.profile = new Profile(overallStart, overallEnd, executionTime, (long) resultList.size(), new Long(diagnostic.get("count").toString()), (long) queryFields.getQueriedColumns().size());


		return resultList;

	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> basicResultsList(Object result, List<Map<String, Object>> resultList, List<String> averageFields, Set<String> distinctCounters) {
		//results come in separated hash maps. This are queries without group by statements
		Map<String, Object> hm = (Map<String, Object>) result;
		Map<String, Object> diagnostic = null;
		if (hm.size() > 0) {
			calculateDistinctCounters(hm, distinctCounters);
			calculateAverages(averageFields, hm);
			replaceLuaLimitValues(hm);
			if (hm.containsKey("sys_")) {
				diagnostic = (HashMap<String, Object>) hm.remove("sys_");
			}

			resultList.add(hm);
		}
		return diagnostic;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> aggregationResultsList(Object result, List<Map<String, Object>> resultList, HavingField having, List<String> averageFields, Set<String> distinctCounters) {
		//all rows are in a single hash map
		Map<String, Map<String, Object>> hm = (Map<String, Map<String, Object>>) result;
		Iterator<Map.Entry<String, Map<String, Object>>> iterator = hm.entrySet().iterator();
		Map<String, Object> diagnostic = null;

		while (iterator.hasNext()) {
			Map.Entry<String, Map<String, Object>> entry = iterator.next();

			if (entry.getKey().equals("sys_")) {
				diagnostic = entry.getValue();

			} else {
				Map<String, Object> values = entry.getValue();
				calculateDistinctCounters(values, distinctCounters);
				calculateAverages(averageFields, values);
				replaceLuaLimitValues(values);

				if (evaluateHavingStatement(having, values)) {
					resultList.add(values);
				}
			}

			iterator.remove();
		}

		return diagnostic;
	}

	private Map<String, Object> convertMapToSortedMap(Map<String, Object> unsortedMap, List<String> fields) {
		Map<String, Object> sortedMap = new LinkedHashMap<>();

		for (String field : fields) {
			sortedMap.put(field, unsortedMap.get(field));
		}
		return sortedMap;

	}

	/**
	 * having statements is evaluated by EvalEx. This method sets variables in having statements with values.
	 */
	private boolean evaluateHavingStatement(HavingField having, Map<String, Object> hm) {
		if (having.getFields().size() > 0) {
			for (String field : having.getFields()) {
				if (hm.get(field) == null) {
					return false;
				}
				having.getExpression().and(field, new BigDecimal(hm.get(field).toString()));
			}
			return having.getExpression().eval().intValue() == 1;
		}
		return true;
	}

	private void replaceLuaLimitValues(Map<String, Object> values) {
		Long minLong = new Long(Definitions.LuaValues.Max.value);
		Long maxLong = new Long(Definitions.LuaValues.Min.value);
		for (String subKey : values.keySet()) {
			if (!subKey.equals("sys_") && values.get(subKey) != null && (values.get(subKey).equals(minLong) || values.get(subKey).equals(maxLong))) {
				values.put(subKey, null);
			}
		}
	}

	/**
	 * takes a size of a hash map with distinct values.
	 */
	private void calculateDistinctCounters(Map<String, Object> values, Set<String> distinctCounters) {
		for (String subKey : values.keySet()) {
			if (!subKey.equals("sys_") && distinctCounters.contains(subKey) && values.get(subKey) instanceof HashMap) {
				values.put(subKey, (long) ((HashMap) values.get(subKey)).size());
			}
		}
	}

	/**
	 * calculate averages for fields
	 *
	 * @param averageFields - field names to calculate averages
	 * @param hm            - values
	 */
	private void calculateAverages(List<String> averageFields, Map<String, Object> hm) {
		if (averageFields.size() > 0) {
			for (String fieldAvg : averageFields) {
				Long counter = (Long) hm.remove(fieldAvg + "_count_");
				counter = counter != 0 ? counter : 1;
				hm.put(fieldAvg, (Long) hm.get(fieldAvg) * 1.0 / counter);
			}
		}
	}

	/**
	 * sort result list
	 */
	private void sortElements(List<Map<String, Object>> list, final OrderField orderField) {
		final List<String> orderList = orderField.getOrderList();

		Collections.sort(list, new Comparator<Map<String, Object>>() {
			public int compare(Map<String, Object> one, Map<String, Object> two) {

				for (String key : orderList) {

					int sortOrder = orderField.getOrderDirection().get(key);

					String first = one != null && one.containsKey(key) && one.get(key) != null ? one.get(key).toString() : sortOrder == 1 ? String.valueOf(Integer.MAX_VALUE) : String.valueOf(Integer.MIN_VALUE);
					String second = two != null && two.containsKey(key) && two.get(key) != null ? two.get(key).toString() : sortOrder == 1 ? String.valueOf(Integer.MAX_VALUE) : String.valueOf(Integer.MIN_VALUE);

					if (one != null && one.containsKey(key) && two != null && two.containsKey(key) && (one.get(key) instanceof Map || two.get(key) instanceof Map || one.get(key) instanceof List || two.get(key) instanceof List)) {
						return 0;
					} else if (one != null && one.containsKey(key) && two != null && two.containsKey(key) && (one.get(key) instanceof String || two.get(key) instanceof String || one.get(key) instanceof Boolean || two.get(key) instanceof Boolean)) {
						return first.compareTo(second) * orderField.getOrderDirection().get(key);
					} else if (!first.equals(second)) {
						return new BigDecimal(first).compareTo(new BigDecimal(second)) * orderField.getOrderDirection().get(key);
					}

				}
				return 0;
			}
		});
	}

	public Profile getProfile() {
		return profile;
	}
}
