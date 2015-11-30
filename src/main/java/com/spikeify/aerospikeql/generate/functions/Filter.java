package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.parse.QueryFields;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by roman on 16/07/15.
 *
 * Logic for filter code in lua script
 */
public class Filter extends Function {

	private Filter() {
		functionName = "where";
		nameArg1 = "topRec";
		streamOperation = "filter";
		level = 1;
		code = "";
	}

	public static Filter factory(QueryFields queryFields) {
		Filter filter = new Filter();
		filter.setFunction(queryFields);
		return filter;
	}

	private void setFunction(QueryFields queryFields) {
		setSignature1Arg();
		if (queryFields != null) {
			code = addLogic(queryFields);
		}
	}

	private String addLogic(QueryFields queryFields) {
		String tabs = getTabs(level + 1);
		String generatedCode = tabs + "return ";

		String statement = queryFields.getWhereField();
		HashSet<String> functions = new HashSet<>(); //function code to include in LUA script

		HashMap<String, String> replaceQuotesMapping = new HashMap<>(); //contains: MATCH1: 'value1', MATCH2: value2. We do not want to split strings in quotes.
		statement = preProcessStatementQuotes(replaceQuotesMapping, statement); //replace strings in quotes with MATCH1, etc. to not split them and set replaceQuotesMapping structure.
		String subCode = super.parseStatement(statement, functions); //convert statements to LUA code
		subCode = processField(subCode);
		subCode = postProcessStatementQuotes(replaceQuotesMapping, subCode); //replace MATCH1, etc. with original strings in quotes

		queryFields.getTransformationFunctions().addAll(functions);
		generatedCode += subCode + "\n";


		return generatedCode;
	}

}
