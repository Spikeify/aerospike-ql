package com.spikeify.aerospikeql.parse;

import com.spikeify.aerospikeql.common.Definitions;
import com.spikeify.aerospikeql.parse.fields.HavingField;
import com.spikeify.aerospikeql.parse.fields.OrderField;
import com.spikeify.aerospikeql.parse.fields.SelectField;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.BasicStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by roman on 16/07/15.
 * <p/>
 * Data structure with fields of SQL query.
 */
public class QueryFields {

	public final String groupBySeparator = Definitions.groupByKeySeparator; //separator for groupBy key, it can be empty string.
	private final SelectField selectField = new SelectField();
	private final OrderField orderFields = new OrderField();
	private final HavingField havingField = new HavingField();
	private final List<String> groupList = new ArrayList<>();
	private final List<String> averages = new ArrayList<>(); //names of fields to calculate averages
	private final HashSet<String> transformationFunctions = new HashSet<>(); //names of functions to add to LUA script
	private final HashSet<String> queriedColumns = new HashSet<>(); //number of columns that are queried
	private String whereField = "";
	private String namespace;
	private String set;
	private int limit = -1; //number of rows to output

	public List<String> getGroupList() {
		return groupList;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public String getWhereField() {
		return whereField;
	}

	public void setWhereField(String whereField) {
		this.whereField = whereField;
	}

	public HashSet<String> getTransformationFunctions() {
		return transformationFunctions;
	}

	public List<String> getAverages() {
		return averages;
	}

	public OrderField getOrderFields() {
		return orderFields;
	}

	public SelectField getSelectField() {
		return selectField;
	}

	public HavingField getHavingField() {
		return havingField;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getSet() {
		return set;
	}

	public void setSet(String set) {
		this.set = set;
	}

	public HashSet<String> getQueriedColumns() {
		return queriedColumns;
	}

	/**
	 * Count number of fields included in a query
	 *
	 * @param statement - part of select statements
	 * @param toAdd     queried columns
	 */

	private void countFields(String statement, List<String> toAdd) {
		String[] fieldsSplit = statement.replace("(", " ").replace(")", " ").split(" ");
		Pattern detectField = Definitions.getDetectFieldPattern();

		for (String field : fieldsSplit) {
			if (!field.contains("\'") &&
							!Definitions.nullValue.equalsIgnoreCase(field) &&
							!Definitions.aggregations.contains(field.toUpperCase()) &&
							!Definitions.transformations.contains(field.toUpperCase()) &&
							!Definitions.transformationsOperators.contains(field.toUpperCase()) &&
							!Definitions.conditionOperators.contains(field.toUpperCase())) {

				Matcher m = detectField.matcher(field);
				if (m.find())
					toAdd.add(m.group(0).trim());
			}
		}
	}

	/**
	 * number of columns read from aerospike
	 */

	private void setNumberOfAccessedFields() {
		//count number of accessed columns
		ArrayList<String> uniqueFields = new ArrayList<>();
		countFields(whereField, uniqueFields);
		queriedColumns.addAll(groupList);
		for (String field : selectField.getSelectList())
			countFields(field, uniqueFields);
		queriedColumns.addAll(uniqueFields);
		queriedColumns.addAll(orderFields.getOrderList());
	}

	/**
	 * group together all averages to calculate them in output results
	 */
	private void setAveragesFields() {
		for (Statement statement : selectField.getStatements()) {
			if (statement instanceof AggregationStatement) {
				AggregationStatement aggregationField = (AggregationStatement) statement;
				if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
					averages.add(aggregationField.getAlias());
				}
			}
		}
	}

	/*
	 * add fields from fieldList to select statements to read them from map
	 */
	private void addFields(List<String> fieldList) {

		if (fieldList.size() > 0) {
			for (String field : fieldList) {
				if (!selectField.getSelectList().contains(field) && !selectField.getAliases().contains(field)) {
					selectField.getSelectList().add(field); //add for safety check
					selectField.getStatements().add(new BasicStatement.BasicFieldBuilder().setAlias(field).setField(field).createBasicField());
				}
			}
		}
	}

	/**
	 * invoked after fields are set
	 *
	 * @throws QueryParserException
	 */

	public void postProcess() throws QueryParserException {
		selectField.setAliases();
		selectField.setFields();  //classify fields (basic, aggregation, transformation)
		havingField.setFields(selectField.getAliases());
		orderFields.setOrderDirection();
		addFields(groupList);
		addFields(orderFields.getOrderList());
		setNumberOfAccessedFields();
		setAveragesFields();

		//every basic field should be in group by
		for (Statement statement : selectField.getStatements()) {
			if (groupList.size() > 0 && statement.isNested() && !groupList.contains(statement.getAlias())) {
				String message = statement.getAlias() + " statements is not in group by statements.";
				throw new QueryParserException(message);
			}
		}

//		for (Statement field : selectField.getStatements()) {
//			if(Definitions.forbiddenFieldNames.contains(field.getField())){
//				String message = field.getField() + " name is forbidden as it used by aerospike-ql.";
//				throw new QueryParserException(message);
//			}
//		}

	  /*
	  //every field from order by should be in select
	  for(String field: orderFields.getOrderList())
	      if(!selectField.getAliases().contains(field)){
	          String message = field + " field in order by is not in select statements.";
	          throw new QueryParserException(message);
	      }
	  */


	}
}
