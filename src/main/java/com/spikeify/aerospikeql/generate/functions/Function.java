package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.common.Definitions;

import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by roman on 16/07/15.

 * abstract class defines common fields and methods for other lua functions.
 */
public class Function {
	protected final String luaSep = ".."; //LUA string concatenation
	protected String code; //code for a function body
	protected String functionName;
	protected String nameArg1; //first argument for a function
	protected String nameArg2;
	protected String nameArg3;
	protected String streamOperation; //filter, map, aggregate or reduce
	protected int level; //code indentation
	private String header; // local function functionName(arg1)
	private String footer; // end
	private String streamArg; //argument for a stream operation

	public String getFunction() {
		return header + code + footer;
	}

	public String getStreamOperation() {
		if (this.streamArg == null) {
			return getStreamOperation1Arg();
		} else {
			return getStreamOperation2Arg();
		}
	}

	public String getStreamOperation1Arg() {
		return " : " + streamOperation + "(" + functionName + ")";
	}

	public String getStreamOperation2Arg() {
		return " : " + streamOperation + "(" + streamArg + ", " + functionName + ")";
	}


	public void setSignature0Arg() {
		String tabs = getTabs(level);
		String code = "";

		if (level == 0) {
			code += tabs + "function ";
		} else {
			code += tabs + "local function ";
		}
		code += functionName + "()\n";
		setHeader(code);
		setFooter(tabs + "end\n\n");
	}

	public void setSignature1Arg() {
		String tabs = getTabs(level);
		String code = "";

		if (level == 0) {
			code += tabs + "function ";
		} else {
			code += tabs + "local function ";
		}
		code += functionName + "(" + nameArg1 + ")\n";
		setHeader(code);
		setFooter(tabs + "end\n\n");
	}

	public void setSignature2Arg() {
		String tabs = getTabs(level);
		String code = "";

		if (level == 0) {
			code += tabs + "function ";
		} else {
			code += tabs + "local function ";
		}
		code += functionName + "(" + nameArg1 + ", " + nameArg2 + ")\n";
		setHeader(code);
		setFooter(tabs + "end\n\n");
	}

	public void setSignature3Arg() {
		String tabs = getTabs(level);
		String code = "";

		if (level == 0) {
			code += tabs + "function ";
		} else {
			code += tabs + "local function ";
		}
		code += functionName + "(" + nameArg1 + ", " + nameArg2 + ", " + nameArg3 + ")\n";
		setHeader(code);
		setFooter(tabs + "end\n\n");
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getFooter() {
		return footer;
	}

	public void setFooter(String footer) {
		this.footer = footer;
	}

	public void setStreamArg(String streamArg) {
		this.streamArg = streamArg;
	}


	/**
	 * Create an indentation with tabs
	 * @param level - level of intendation
	 * @return - string with tab intendations
	 */
	protected String getTabs(int level) {
		String tabs = "";
		for (int i = 0; i < level; i++)
			tabs += "\t";
		return tabs;
	}

	/**
	 * Parse statements (examples statements is 1 + 2 + value)
	 *
	 * @param statement - transformation in select statements, e.g. (1+2)/3 + hour(timestamp) + 400
	 * @param functions - detected functions in statements, e.g. HOUR, DAY
	 * @return - LUA code for a statements.
	 */
	public String parseStatement(String statement, HashSet<String> functions) {
		String[] fields = statement.split(Definitions.transformationOperatorsRegex(true)); //split a statements with positive lookahead. This means: "a, b, c".split(",") => ["a,", "b,", "c"]
		String item = fields[0].trim();

		if (item.equalsIgnoreCase("condition()")) {
			if (fields.length > 1) {
				return item + parseStatement(arrayToString(fields, 1), functions);
			} else {
				return item;
			}
		}

		if (item.equals("") || item.matches(Definitions.defaultQuotesReplacementName + "\\d+")) {
			return item; //item empty, condition() or MATCH1
		}

		if (Definitions.transformationsOperators.contains(item.toUpperCase())) {
			return " " + item.toLowerCase() + " "; //operator +, - etc.
		}

		if (Definitions.transformations.contains(item.toUpperCase())) {
			functions.add(item.toLowerCase()); //add LUA function code for transformation
			return item.toLowerCase();
		}

		//split: + field1 etc.
		String[] itemSplit = item.split(Definitions.transformationOperatorsRegex(false));
		if (itemSplit.length > 1)
			return parseStatement(itemSplit[0], functions) + //first part of item
							parseStatement(arrayToString(itemSplit, 1), functions) + //second part of item
							parseStatement(arrayToString(fields, 1), functions); //all others

		//process parentheses
		Pattern detectLeftParenthesis = Pattern.compile("(.*)\\((.+)"); //value before right parenthesis and after
		Matcher matchLeftParenthesis = detectLeftParenthesis.matcher(item);
		if (matchLeftParenthesis.find()) {
			return parseStatement(matchLeftParenthesis.group(1), functions) + "(" +
							parseStatement(matchLeftParenthesis.group(2), functions) +
							parseStatement(arrayToString(fields, 1), functions);
		}

		Pattern detectRightParenthesis = Pattern.compile("(.+)\\)");
		Matcher matchRightParenthesis = detectRightParenthesis.matcher(item);
		if (matchRightParenthesis.find()) {
			return parseStatement(matchRightParenthesis.group(1), functions) + ")";
		}


		Pattern detectFieldPattern = Definitions.getDetectFieldPattern();
		Matcher detectFieldMatcher = detectFieldPattern.matcher(item);

		if (detectFieldMatcher.find()) {
			if (item.contains("like_")) {
				return processLikeStatement(item) + parseStatement(arrayToString(fields, 1), functions); //item is a field in AS;
			} else {
				return "topRec[\"" + item + "\"]" + parseStatement(arrayToString(fields, 1), functions); //item is a field in AS
			}
		}


		return statement.trim();

	}

	/**
	 * Generate LUA code for SQL like and not like
	 *
	 * @param item is field1_like_field2
	 * @return Lua code
	 */

	private String processLikeStatement(String item) {
		Pattern detectFieldPattern = Definitions.getDetectFieldPattern();
		String operation = item.contains("_notlike_") ? "not " : "";
		operation += "string.match(";
		String[] itemSplit = item.split("_like_|_notlike_");

		boolean addComma = false;
		for (String subItem : itemSplit) {
			Matcher detectFieldMatcher = detectFieldPattern.matcher(subItem);

			if (detectFieldMatcher.find()) {
				if (subItem.matches(Definitions.defaultQuotesReplacementName + "\\d+"))
					operation += subItem;
				else
					operation += "topRec[\"" + subItem + "\"]";
				if (!addComma) {
					operation += ", ";
					addComma = true;
				}
			} else
				operation += subItem;

		}
		return operation + ") ";

	}

	/**
	 * join strings in array to a string and return it
	 *
	 * @param stringArray ["val1", "val2"]
	 * @param first       start index for joining string
	 * @return string
	 */

	private String arrayToString(String[] stringArray, int first) {
		String newString = "";
		for (int i = first; i < stringArray.length; i++)
			newString += stringArray[i];
		return newString;
	}

	/**
	 * @param replaceQuotesMapping - is empty on input and it stores {MATCH1: 'event/start ...}
	 * @param statement            - example: field1 is gt field2
	 * @return - processed statements
	 */

	public String preProcessStatementQuotes(Map<String, String> replaceQuotesMapping, String statement) {
		statement = statement.replaceAll("([^!><])=", "$1 == ") //replace: = with ==, but not if !=.
						.replaceAll(" (?i)NOT (?i)LIKE ", "_notlike_") //replace: field1 not like 'start' with field1_notlike_start (ignore case)
						.replaceAll(" (?i)LIKE ", "_like_") //replace: field1 like 'start' with field1like_start (ignore case)
						.replaceAll(" (?i)and ", " AND ") //replace and with AND (ignore case), to be able to use contains method on arrayList.
						.replaceAll(" (?i)or ", " OR ");

		Pattern pattern = Pattern.compile("('.*?')"); //matches all values between ' '
		Matcher matcher = pattern.matcher(statement);

		//replace all values between quotes with MATCH%d
		int i = 0;
		while (matcher.find()) {
			String match = matcher.group(1);
			String newName = Definitions.defaultQuotesReplacementName + String.valueOf(i++);
			replaceQuotesMapping.put(newName, match);
			statement = statement.replace(match, newName);
		}
		return statement;
	}


	/**
	 * replace MATCH1 with 'value1' etc.
	 * replace (topRec[timestamp] or 0) == (topRec[localtimestamp] or 0) with topRec[timestamp] == topRec[localtimestamp]
	 *
	 * @param replaceQuotesMapping it stores {MATCH1: 'event/start ...}
	 * @param statement            example: field1 is gt field2
	 * @return postprocessed statements
	 */

	public String postProcessStatementQuotes(Map<String, String> replaceQuotesMapping, String statement) {
		statement = statement.replaceAll("(?i)CONDITION\\(\\)", "\\(load\\('return '.. runTimeCondition, '', 't', \\{topRec=topRec, string=string\\}\\)\\)\\(\\)"); //replace condition(): with code to run runtime condition.
		if (replaceQuotesMapping.size() > 0)
			for (Map.Entry<String, String> entry : replaceQuotesMapping.entrySet())
				statement = statement.replace(entry.getKey(), entry.getValue());

		statement = statement.replaceAll("!=", "~=")
						.replaceAll("< =", "<=")
						.replaceAll("> =", ">=")
						.replaceAll("(?i)topRec\\[\"null\"\\]", "nil");

		return statement;
	}

	public String processField(String field) {
		for (String recordFieldTransformation : Definitions.recordFieldTransformations) {
			field = field.replaceAll("(?i)(" + recordFieldTransformation + ")\\(\\)", "$1\\(" + nameArg1 + "\\)");
		}
		return field;
	}

}
