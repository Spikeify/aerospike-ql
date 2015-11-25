package com.spikeify.aerospikeql.common;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by roman on 17/07/15.

 * Definitions contains constants and function names.
 */
public class Definitions {

	public static final String defaultQuotesReplacementName = "MATCH";
	public static final String groupByKeySeparator = ":"; //separator for groupBy key, it can be empty string.
	public static final String nullValue = "null"; //when field had no value, we replace it with nullValue
	public static final String millisToSec = String.valueOf(1000); //currentTime is in millis;

	public static final List<String> aggregations = new ArrayList<String>() {{
		add("MAX");
		add("MIN");
		add("AVG");
		add("SUM");
		add("COUNT");
	}};

	public static final List<String> noFieldTransformations = new ArrayList<String>() {{
		add("NOW"); //time in millis
		add("CURRENT_DATE"); //date in %Y-%m-%d.
		add("CURRENT_TIME"); //date in %H:%M:%S.
		add("CURRENT_TIMESTAMP"); //date in %Y-%m-%d %H:%M:%S UTC.
	}};

	//functions remove a time unit from a timestamp
	public static final List<String> singleFieldTransformationsUnitRemove = new ArrayList<String>() {{
		add("UTC_MS_TO_SECOND");
		add("UTC_MS_TO_MINUTE");
		add("UTC_MS_TO_HOUR");
		add("UTC_MS_TO_DAY");
//		add("UTC_MS_TO_WEEK");
//		add("UTC_MS_TO_MONTH");
//		add("UTC_MS_TO_YEAR");
	}};

	//functions extract a time unit from a timestamp
	public static final List<String> singleFieldTransformationsUnitExtraction = new ArrayList<String>() {
		{
			add("SECOND");
			add("MINUTE");
			add("HOUR");
			add("DAY");
			add("MONTH");
			add("YEAR");
		}
	};

	public static final List<String> recordFieldTransformations = new ArrayList<String>() {{
		add("PRIMARY_KEY");
		add("TTL");
		add("GENERATION");
		add("DIGEST");
	}};

	public static final List<String> singleFieldTransformations = new ArrayList<String>() {{
		add("INTEGER");
		add("FLOAT");
		add("DATE"); //convert timestamp to date
		add("MSEC_TO_TIMESTAMP");
		add("TIME");
		addAll(recordFieldTransformations);
		addAll(singleFieldTransformationsUnitExtraction);
		addAll(singleFieldTransformationsUnitRemove);
	}};

	public static final List<String> doubleFieldTransformations = new ArrayList<String>() {{
		add("DATEDIFF_MS");
		add("DATEDIFF");
		add("JSON_EXTRACT_SCALAR");
		add("LIST_CONTAINS");
		add("LIST_MATCH");
		add("LIST_RETRIEVE");
		add("STRING_CONTAINS");
		add("STRING_MATCH");
		add("REGEXP_MATCH");
		add("IFNULL");
	}};

	public static final List<String> transformations = new ArrayList<String>() {{
		addAll(noFieldTransformations);
		addAll(singleFieldTransformations);
		addAll(doubleFieldTransformations);
	}};

	public static final List<String> stopWords = new ArrayList<String>() {{
		add("AND");
		add("OR");
	}};

	public static final List<String> conditionOperators = new ArrayList<String>() {{
		add("DISTINCT");
		add("CASE");
		add("WHEN");
		add("IF");
		add("ELSE");
		add("THEN");
		add("END");
		add("LIKE");
		add("NOT");
	}};

	public static final List<String> transformationsOperators = new ArrayList<String>() {{
		add("\\*");
		add("\\+");
		add("/");
		add("-");
		add("%");
		add("AND");
		add("OR");
		add(",");
		add("!=");
		add("==");
		add("<");
		add(">");
	}};

	public static final Map<String, String> convertMillisToSec = new HashMap<String, String>() {{
		put("SECOND", "1000");
		put("MINUTE", "60000");
		put("HOUR", "3600000");
		put("DAY", "86400000");
//		put("WEEK", "604800000");
//		put("MONTH", "2629740000");
//		put("YEAR", "31560000000");
	}};

	public static final Set<String> luaReservedWords = new HashSet<String>() {{
		add("and");
		add("break");
		add("do");
		add("else");
		add("elseif");
		add("end");
		add("false");
		add("for");
		add("function");
		add("if");
		add("in");
		add("local");
		add("nil");
		add("not");
		add("or");
		add("repeat");
		add("return");
		add("then");
		add("true");
		add("until");
		add("while");
	}};
	public static Set<String> forbiddenFieldNames = new HashSet<String>() {{
		addAll(aggregations);
		add(defaultQuotesReplacementName);
		addAll(transformations);
		addAll(stopWords);
		addAll(conditionOperators);
		addAll(transformationsOperators);
		addAll(convertMillisToSec.keySet());
		addAll(luaReservedWords);
	}};

	public static Pattern getPattern(List<String> fields) {
		String regex = "";

		for (String field : fields) {
			regex += "(^" + field + ")\\((.+)\\)|";
		}
		regex = regex.substring(0, regex.length() - 1);

		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}

	public static Pattern getDetectFieldPattern() {
		return Pattern.compile("^[a-z][a-z_-]+", Pattern.CASE_INSENSITIVE);
	}

	public static String replaceCondition(String condition) {
		//replace operators to match lua operators
		return condition.replace(" = ", " == ").replace(" != ", " ~= ");
	}

	public static String transformationOperatorsRegex(boolean lookahead) {
		String regex = lookahead ? "(?=" : "(?<=";

		for (String operator : transformationsOperators)
			regex += operator + "|";
		regex = regex.substring(0, regex.length() - 1) + ")";
		return regex;
	}

	public static boolean isSelectAll(List<String> selectFields) {
		if (selectFields != null) {
			for (String field : selectFields) {
				if (field.equalsIgnoreCase("*")) {
					return true;
				}
			}
		}
		return false;
	}

	public enum LuaValues {
		Min("922337203685477632"), Max("-922337203685477632");

		public final String value;

		LuaValues(String value) {
			this.value = value;
		}
	}
}



























