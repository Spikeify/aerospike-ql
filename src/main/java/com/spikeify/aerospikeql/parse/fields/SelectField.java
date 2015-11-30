package com.spikeify.aerospikeql.parse.fields;

import com.spikeify.aerospikeql.Definitions;
import com.spikeify.aerospikeql.parse.ParserException;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.BasicStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import com.spikeify.aerospikeql.parse.fields.statements.TransformationStatement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by roman on 17/07/15.
 *
 * Select field data structure is a sub structure of query statements.
 *
 * Select statements can contain statements, field transformations, field aggregations.
 */
public class SelectField {

	private final List<Statement> statements = new ArrayList<>(); //main data structure with select statements
	private final List<String> aliases = new ArrayList<>(); //aliases of statements that are print out
	private List<String> selectList = new ArrayList<>(); //helper data structure:  it is used to fill statements data structure
	private final Set<String> distinctCounters = new HashSet<>();
	private boolean aggregations = false; //if query contains aggregations

	/**
	 * Split every select field on AS and set aliases
	 *
	 * @throws ParserException - alias has incorrect name
	 */
	public void setAliases() throws ParserException {
		List<String> newSelectList = new ArrayList<>();

		for (String field : selectList) {
			String[] names = field.split(" AS ");

			//without alias
			if (names.length == 1) {
				if (names[0].length() > 14) {
					String message = "Statement " + names[0] + " is too long. Please define an alias.";
					throw new ParserException(message);

				} else if (names[0].contains("(")) {
					String message = "Please define an alias for field " + names[0] + ".";
					throw new ParserException(message);
				}
				aliases.add(names[0]);

				//with alias
			} else if (names.length == 2) {
				if (names[1].length() > 14) {
					String message = "Alias " + names[1] + " is too long. Please define shorter alias.";
					throw new ParserException(message);
				} else if (names[1].endsWith("_")) {
					String message = "Aliases should not end with _ character: " + names[1] + ".";
					throw new ParserException(message);
				}
				aliases.add(names[1]);
			}
			newSelectList.add(names[0]);
		}

		this.selectList = newSelectList;
	}

	/**
	 * classify statements in select statements. We have 3 types of statements: basic, transformation and aggregation.
	 * <p/>
	 * basic: select timestamp
	 * transformation: select hour(timestamp) as hour
	 * aggregation: sum(duration) as sumDuration
	 */

	public void setFields() {
		Pattern patternAggregations = Definitions.getAggregationsPattern();
		Pattern patternDistinct = Pattern.compile("DISTINCT (.+)", Pattern.CASE_INSENSITIVE);
		Pattern patternIf = Pattern.compile("CASE WHEN (.*) THEN (.*) END", Pattern.CASE_INSENSITIVE);
		Pattern patternIfElse = Pattern.compile("CASE WHEN (.*) THEN (.*) ELSE (.*) END", Pattern.CASE_INSENSITIVE);

		String match, condition, condTrue, condFalse, operation = "";

		int index = 0;
		for (String field : selectList) {
			Matcher matcherAggregations = patternAggregations.matcher(field);

			if (matcherAggregations.find()) {
				//aggregation statements
				aggregations = true; //query has aggregations
				int j = 0;

				for (int i = 1; i <= matcherAggregations.groupCount(); i++)
					if ((match = matcherAggregations.group(i)) != null) {
						if (j % 3 == 0) {
							operation = match.toLowerCase(); //operation name: sum, avg, etc.

						} else if (j % 3 == 1) {
							Matcher matcherDistinct = patternDistinct.matcher(match);
							Matcher matcherIf = patternIf.matcher(match);
							Matcher matcherIfElse = patternIfElse.matcher(match);

							if (matcherDistinct.find()) {
								//count distinct
								match = matcherDistinct.group(1);
								match = evaluateCondition(match, index);

								distinctCounters.add(aliases.get(index));
								statements.add(new AggregationStatement.AggregationFieldBuilder(aliases.get(index), operation).setField(match).createAggregationField());
							} else if (matcherIfElse.find()) {
								//if else aggregation
								condition = Definitions.replaceCondition(matcherIfElse.group(1));
								condition = evaluateCondition(condition, index);

								condTrue = matcherIfElse.group(2);
								condTrue = evaluateCondition(condTrue, index);

								condFalse = matcherIfElse.group(3);
								condFalse = evaluateCondition(condFalse, index);

								statements.add(new AggregationStatement.AggregationFieldBuilder(aliases.get(index), operation).setCondition(condition).setIsTrue(condTrue).setIsFalse(condFalse).createAggregationField());

							} else if (matcherIf.find()) {
								//if aggregation
								condition = Definitions.replaceCondition(matcherIf.group(1));
								condition = evaluateCondition(condition, index);

								condTrue = matcherIf.group(2);
								condTrue = evaluateCondition(condTrue, index);

								statements.add(new AggregationStatement.AggregationFieldBuilder(aliases.get(index), operation).setCondition(condition).setIsTrue(condTrue).createAggregationField());

							} else {
								//basic aggregation: min(timestamp)
								match = evaluateCondition(match, index);
								statements.add(new AggregationStatement.AggregationFieldBuilder(aliases.get(index), operation).setField(match).createAggregationField());
							}
							break; //when aggregation object is set, break from the loop

						}
						j++;
					}

			} else if (field.contains("(") || field.contains("+") || field.contains("-") || field.contains("*") || field.contains("/") || field.contains("%")) {
				//transformation statements
				statements.add(new TransformationStatement.TransformationFieldBuilder().setAlias(aliases.get(index)).setCondition(field).setNested(true).createTransformationField());

			} else {
				//basic field
				statements.add(new BasicStatement.BasicFieldBuilder().setAlias(aliases.get(index)).setField(field).setNested(true).createBasicField());

			}

			index++;

		}


	}


	/**
	 * Determine if string is a field or a constant (function name)
	 */

	private void findFields(String statement, List<String> toAdd) {
		String match;
		String[] fieldsSplit = statement.replace("(", " ").replace(")", " ").split(" ");
		Pattern detectField = Definitions.getDetectFieldPattern();

		for (String field : fieldsSplit) {
			if (field.contains("\'"))
				continue; //omit constants that are marked with quotes. E.g. the second parameter of JSON_EXTRACT
			Matcher m = detectField.matcher(field);
			if (m.find()) {
				match = m.group(0);
				if (!selectList.contains(match) &&
								!toAdd.contains(match) &&
								!Definitions.transformations.contains(match.toUpperCase()) &&
								!Definitions.stopWords.contains(match.toUpperCase()) &&
								!Definitions.nullValue.equalsIgnoreCase(match))
					toAdd.add(match);

			}
		}
	}

	/**
	 * method checks if a condition have statements with transformations or it has just basic statements
	 *
	 * @param condition is a part of condition. E.g. hour(timestamp) > 100
	 * @param index     of an alias
	 * @return condition with new field names (because of transformations).
	 */

	private String evaluateCondition(String condition, int index) {
		if (isTransformation(condition)) {
			condition = setTransformation(condition, index);

		} else {
			List<String> newFields = new ArrayList<>();
			findFields(condition, newFields);
			for (String subField : newFields)
				statements.add(new BasicStatement.BasicFieldBuilder().setAlias(subField).setField(subField).createBasicField());
		}
		return condition;
	}

	/**
	 * @param condition is a part of condition. E.g. hour(timestamp) > 100
	 * @param index     of an alias
	 * @return condition with new field names (because of transformations).
	 */

	private String setTransformation(String condition, int index) {
		List<String> newFields = new ArrayList<>();
		String alias = aliases.get(index);

		findFields(condition, newFields); //get all field names from condition
		List<String> transformations = getTransformations(condition); //get all transformations from condition

		int k = 0;
		for (String subField : newFields) {
			String newFieldName = alias + "_" + subField; //mark transformed statements with new name
			condition = condition.replace(transformations.get(k), newFieldName); //replace the name with new name in condition
			statements.add(new TransformationStatement.TransformationFieldBuilder().setAlias(newFieldName).setCondition(transformations.get(k)).createTransformationField());
			k++;
		}

		return condition;
	}

	/**
	 * @param match is a part of condition. E.g. hour(timestamp) > 100
	 * @return true if match contains transformation function
	 */

	private boolean isTransformation(String match) {
		for (String transformation : Definitions.transformations)
			if (match.toUpperCase().contains(transformation + "("))
				return true;
		return false;
	}

	/**
	 * @param match is a part of condition. Example hour(timestamp) > 10 or hour(timestamp1) > hour(timestamp2)
	 * @return all transformations
	 */

	private List<String> getTransformations(String match) {

		List<String> extractedTransformations = new ArrayList<>();

		for (String field : match.split("[<>=!]")) { //split match only with operators and not with a space.
			for (String transformation : Definitions.transformations)
				if (field.toUpperCase().startsWith(transformation + "(")) {
					extractedTransformations.add(field);
					break;
				}
		}
		return extractedTransformations;

	}

	public List<String> getAliases() {
		return aliases;
	}

	public List<Statement> getStatements() {
		return statements;
	}

	public boolean isAggregations() {
		return aggregations;
	}

	public void setAggregations(boolean aggregations) {
		this.aggregations = aggregations;
	}

	public List<String> getSelectList() {
		return selectList;
	}

	public Set<String> getDistinctCounters() {
		return distinctCounters;
	}
}
