package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.common.Definitions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by roman on 24/07/15.

 * Logic for transformation functions (DATEDIFF, etc.).
 */
public class Transformation extends Function {

	public Transformation() {
		nameArg1 = "value1";
		nameArg2 = "value2";
		streamOperation = "";
		level = 1;
	}

	public static Transformation factory(String funName) {
		Transformation transformation = new Transformation();
		transformation.setFunction(funName);
		return transformation;
	}

	public void setFunction(String functionName) {
		this.functionName = functionName;
		String generatedCode = "";

		if (Definitions.noFieldTransformations.contains(functionName.toUpperCase())) {
			setSignature0Arg();
			if (functionName.toUpperCase().equalsIgnoreCase("NOW")) {
				generatedCode = addNowLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("CURRENT_DATE")) {
				generatedCode = addCurrentDateLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("CURRENT_TIME")) {
				generatedCode = addCurrentTimeLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
				generatedCode = addCurrentTimestampLogic();
			}

		} else if (Definitions.singleFieldTransformations.contains(functionName.toUpperCase())) {
			setSignature1Arg();

			if (functionName.equalsIgnoreCase("FLOAT")) {
				generatedCode = addConvertToFloatLogic();
			} else if (functionName.equalsIgnoreCase("INTEGER")) {
				generatedCode = addConvertToIntegerLogic();
			} else if (functionName.equalsIgnoreCase("SECOND")) {
				generatedCode = addConvertToSecondLogic();
			} else if (functionName.equalsIgnoreCase("MINUTE")) {
				generatedCode = addConvertToMinuteLogic();
			} else if (functionName.equalsIgnoreCase("HOUR")) {
				generatedCode = addConvertToHourLogic();
			} else if (functionName.equalsIgnoreCase("DAY")) {
				generatedCode = addConvertToDayLogic();
			} else if (functionName.equalsIgnoreCase("MONTH")) {
				generatedCode = addConvertToMonthLogic();
			} else if (functionName.equalsIgnoreCase("YEAR")) {
				generatedCode = addConvertToYearLogic();
			} else if (functionName.equalsIgnoreCase("MSEC_TO_TIMESTAMP")) {
				generatedCode = addConvertMillisToTimestampLogic();
			} else if (functionName.equalsIgnoreCase("DATE")) {
				generatedCode = addDateLogic();
			} else if (functionName.equalsIgnoreCase("TIME")) {
				generatedCode = addTimeLogic();
			} else if (Definitions.singleFieldTransformationsUnitRemove.contains(functionName.toUpperCase())) {
				generatedCode = removeMillisFromTimestamp(functionName);
			} else if (functionName.equalsIgnoreCase("PRIMARY_KEY")) {
				generatedCode = addPrimaryKeyLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("TTL")) {
				generatedCode = addTTLLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("GENERATION")) {
				generatedCode = addGenerationLogic();
			} else if (functionName.toUpperCase().equalsIgnoreCase("DIGEST")) {
				generatedCode = addDigestLogic();
			}

		} else {
			setSignature2Arg();
			if (functionName.equalsIgnoreCase("DATEDIFF")) {
				generatedCode = addDateDiffLogic();
			} else if (functionName.equalsIgnoreCase("DATEDIFF_MS")) {
				generatedCode = addDateDiffMSLogic();
			} else if (functionName.equalsIgnoreCase("JSON_EXTRACT_SCALAR")) {
				generatedCode = addJsonExtractScalarLogic();
			} else if (functionName.equalsIgnoreCase("REGEXP_MATCH")) {
				generatedCode = addRegexpLogic();
			} else if (functionName.equalsIgnoreCase("IFNULL")) {
				generatedCode = addIfNullLogic();
			} else if (functionName.equalsIgnoreCase("LIST_CONTAINS")) {
				generatedCode = addListContainsLogic();
			} else if (functionName.equalsIgnoreCase("LIST_RETRIEVE")) {
				generatedCode = addListRetrieveLogic();
			} else if (functionName.equalsIgnoreCase("LIST_MATCH")) {
				generatedCode = addListMatchLogic();
			} else if (functionName.equalsIgnoreCase("STRING_CONTAINS")) {
				generatedCode = addStringContainsLogic();
			} else if (functionName.equalsIgnoreCase("STRING_MATCH")) {
				generatedCode = addStringRetrieveLogic();
			}
		}
		code = generatedCode;
	}

	private String addDigestLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return record.digest(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String addGenerationLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return record.ttl(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String addTTLLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return record.gen(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String addPrimaryKeyLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return record.key(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String addStringRetrieveLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return " + nameArg1 + " and string.match(" + nameArg1 + ", " + nameArg2 + ")\n";
		return generatedCode;
	}

	private String addStringContainsLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		generatedCode += tabs1 + "return " + nameArg1 + " and string.match(" + nameArg1 + ", " + nameArg2 + ") == " + nameArg2 + " or false \n";
		return generatedCode;
	}

	private String addListRetrieveLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);
		generatedCode += tabs1 + "if " + nameArg1 + " ~= nil then\n";
		generatedCode += tabs1 + "for item in list.iterator(" + nameArg1 + ") do\n";
		generatedCode += tabs2 + "if item == " + nameArg2 + " then\n";
		generatedCode += tabs3 + "return item\n";
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return ''\n";
		return generatedCode;
	}

	private String addListContainsLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);
		generatedCode += tabs1 + "if " + nameArg1 + " ~= nil then\n";
		generatedCode += tabs1 + "for item in list.iterator(" + nameArg1 + ") do\n";
		generatedCode += tabs2 + "if item == " + nameArg2 + " then\n";
		generatedCode += tabs3 + "return true\n";
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return false\n";
		return generatedCode;
	}

	private String addListMatchLogic() {
		String generatedCode = "";
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);
		generatedCode += tabs1 + "if " + nameArg1 + " ~= nil then\n";
		generatedCode += tabs1 + "for item in list.iterator(" + nameArg1 + ") do\n";
		generatedCode += tabs2 + "if item ~= nil and string.match(item, " + nameArg2 + ") then\n";
		generatedCode += tabs3 + "return true\n";
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return false\n";
		return generatedCode;
	}

	private String addIfNullLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "return " + nameArg1 + " or " + nameArg2 + "\n";
		return generatedCode;
	}

	private String addRegexpLogic() {
		String generatedCode = addStringContainsLogic();
		return generatedCode;
	}

	private String addTimeLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local hour,min,sec = " + nameArg1 + ":match(\".* (%d+):(%d+):(%d+).*\")\n";
		generatedCode += tabs + "return hour .. ':' .. min .. ':' .. sec\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addCurrentTimestampLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "local date = os.date(\"%Y-%m-%d %H:%M:%S\", currentTimestamp /" + Definitions.millisToSec + ")\n";
		generatedCode += tabs + "local year,month,day,hour,min,sec = date:match(\"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)\")\n";
		generatedCode += tabs + "return year.. '-' .. month.. '-' .. day .. ' ' .. hour .. ':' .. min .. ':' .. sec .. ' UTC'\n";
		return generatedCode;
	}

	private String addCurrentTimeLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "local date = os.date(\"%Y-%m-%d %H:%M:%S\", currentTimestamp /" + Definitions.millisToSec + ")\n";
		generatedCode += tabs + "local hour,min,sec = date:match(\"%d+-%d+-%d+ (%d+):(%d+):(%d+)\")\n";
		generatedCode += tabs + "return hour .. ':' .. min .. ':' .. sec\n";
		return generatedCode;
	}

	private String addCurrentDateLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "local date = os.date(\"%Y-%m-%d %H:%M:%S\", currentTimestamp /" + Definitions.millisToSec + ")\n";
		generatedCode += tabs + "local year,month,day=date:match(\"(%d+)-(%d+)-(%d+) %d+:%d+:%d+\")\n";
		generatedCode += tabs + "return year.. '-' .. month.. '-' .. day\n";
		return generatedCode;
	}

	private String addDateLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local year,month,day=" + nameArg1 + ":match(\"(%d+)-(%d+)-(%d+).*\")\n";
		generatedCode += tabs + "return year.. '-' .. month.. '-' .. day\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;

	}

	private String addConvertToDayLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local day=" + nameArg1 + ":match(\"%d+-%d+-(%d+).*\")\n";
		generatedCode += tabs + "return tonumber(day)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToMonthLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local month=" + nameArg1 + ":match(\"%d+-(%d+)-%d+.*\")\n";
		generatedCode += tabs + "return tonumber(month)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToYearLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local year=" + nameArg1 + ":match(\"(%d+)-%d+-%d+.*\")\n";
		generatedCode += tabs + "return tonumber(year)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToSecondLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local sec=" + nameArg1 + ":match(\".*%d+:%d+:(%d+).*\")\n";
		generatedCode += tabs + "return tonumber(sec)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToMinuteLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local min=" + nameArg1 + ":match(\".*%d+:(%d+):%d+.*\")\n";
		generatedCode += tabs + "return tonumber(min)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToHourLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "if " + nameArg1 + " then\n";
		generatedCode += tabs + "local hour=" + nameArg1 + ":match(\".*(%d+):%d+:%d+.*\")\n";
		generatedCode += tabs + "return tonumber(hour)\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "return ''\n";
		return generatedCode;
	}

	private String addConvertToIntegerLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "-- first check for exception then value is rounded down\n";
		generatedCode += tabs + "return pcall(math.floor, " + nameArg1 + ") and math.floor(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String addConvertToFloatLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "-- floats are not supported in UDF. Value is converted to int.\n";
		generatedCode += tabs + "return tonumber(" + nameArg1 + ")\n";
		return generatedCode;
	}

	private String removeMillisFromTimestamp(String timeValue) {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		timeValue = timeValue.toUpperCase();

		Pattern patternTimeUnit = Pattern.compile("UTC_MS_TO_(.+)", Pattern.CASE_INSENSITIVE);
		Matcher matcherTimeUnit = patternTimeUnit.matcher(timeValue);
		if (matcherTimeUnit.find()) {
			String timeUnit = matcherTimeUnit.group(1);
			generatedCode += tabs + "return " + nameArg1 + " and " + nameArg1 + " - (" + nameArg1 + " % " + Definitions.convertMillisToSec.get(timeUnit) + ") or 0\n";
		}
		return generatedCode;
	}

	private String addConvertMillisToTimestampLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "return " + nameArg1 + " and os.date(\"%Y-%m-%d %H:%M:%S\", " + nameArg1 + "/1000) or nil\n";
		return generatedCode;
	}


	private String addDateDiffMSLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "-- day is rounded down\n";
		generatedCode += tabs + "return " + nameArg1 + " and " + nameArg2 + " and math.floor((" + nameArg1 + " - " + nameArg2 + ")/" + Definitions.convertMillisToSec.get("DAY") + ") or nil\n";
		return generatedCode;
	}

	private String addDateDiffLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "local date1 = os.date(\"%Y-%m-%d %H:%M:%S\", currentTimestamp /" + Definitions.millisToSec + ")\n";
		return generatedCode;
	}


	private String addJsonExtractScalarLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "return " + nameArg1 + "[" + nameArg2 + "]\n";
		return generatedCode;
	}

	private String addNowLogic() {
		String generatedCode = "";
		String tabs = getTabs(level + 1);
		generatedCode += tabs + "return currentTimestamp\n"; //currentTimestamp is set in TopLevel class
		return generatedCode;
	}

}
