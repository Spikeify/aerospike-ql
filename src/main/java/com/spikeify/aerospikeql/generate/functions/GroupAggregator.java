package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.common.Definitions;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.BasicStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import com.spikeify.aerospikeql.parse.fields.statements.TransformationStatement;

import java.util.HashSet;
import java.util.List;

/**
 * Created by roman on 16/07/15.

 * Logic for aggregator code in lua script
 */
public class GroupAggregator extends Function {

	private GroupAggregator() {
		functionName = "group_aggregator";
		nameArg1 = "out";
		nameArg2 = "topRec";
		streamOperation = "aggregate";
		level = 1;
		code = "";
	}

	public static GroupAggregator factory(QueryFields queryFields) {
		GroupAggregator groupAggregator = new GroupAggregator();
		groupAggregator.setFunction(queryFields);
		return groupAggregator;
	}

	private void setFunction(QueryFields queryFields) {

		setStreamArg("map()");
		setSignature2Arg();

		if (!queryFields.getSelectField().isAggregations()) {
			code += addLogic(queryFields);
		} else {
			code += addAggregationLogic(queryFields);
		}
	}

	private String groupFieldsCode(QueryFields queryFields, boolean groupBy) {
		String tabs = getTabs(level + 1);
		String subTabs = getTabs(level + 2);
		List<String> groupList = queryFields.getGroupList();
		String sep = queryFields.groupBySeparator, subCode = "", key = "";
		int i = 0;

		if (groupBy) {
			subCode += tabs + "-- grouping operation\n";
			for (String field : groupList) {
				subCode += tabs + "local " + field + " = topRec[\"" + field + "\"]\n";
				key += "tostring(" + field + ")";

				if (i + 1 < groupList.size()) {
					key += super.luaSep + "\"" + sep + "\"" + super.luaSep;
				}
				i++;
			}
			subCode += tabs + "local groupBy = " + key + "\n";
			subCode += "\n";
		}

		HashSet<String> alreadyAdded = new HashSet<>();
		List<Statement> statements = queryFields.getSelectField().getStatements();

		for (Statement statement : statements) {
			if (!alreadyAdded.contains(statement.getAlias()) && !groupList.contains(statement.getAlias()) && (statement instanceof TransformationStatement || statement instanceof BasicStatement)) {
				subCode += tabs + "local " + statement.getAlias() + " = topRec[\"" + statement.getAlias() + "\"]\n";
				alreadyAdded.add(statement.getAlias());
			}

		}
		if (groupBy) {
			subCode += tabs + "if out[groupBy] == nil then\n";
			subCode += subTabs + "out[groupBy] = map()\n";
			for (String field : queryFields.getGroupList())
				subCode += subTabs + "out[groupBy][\"" + field + "\"] = " + field + "\n";
			subCode += tabs + "end\n";
		}

		return subCode;

	}

	private String addSystemDiagnosticsLogic() {
		String tabs = getTabs(level + 1);
		String subTabs = getTabs(level + 2);
		String generatedCode = "";
		generatedCode += tabs + "-- system diagnostics\n";
		generatedCode += tabs + "if out[\"sys_\"] == nil then\n";
		generatedCode += subTabs + "out[\"sys_\"] = map()\n";
		generatedCode += subTabs + "out[\"sys_\"][\"count\"] = 0\n";
		generatedCode += tabs + "end\n";
		generatedCode += tabs + "out[\"sys_\"][\"count\"] = out[\"sys_\"][\"count\"] + 1\n\n";
		return generatedCode;
	}


	private String addLogic(QueryFields queryFields) {
		String tabs = getTabs(level + 1);
		String generatedCode = "";
		generatedCode += addSystemDiagnosticsLogic();
		generatedCode += groupFieldsCode(queryFields, true);
		generatedCode += tabs + "return out\n";
		return generatedCode;

	}

	private String addAggregationLogic(QueryFields queryFields) {
		String tabs = getTabs(level + 1);
		String groupBy = queryFields.getGroupList().size() == 0 && queryFields.getSelectField().isAggregations() ? "" : "[groupBy]";
		String generatedCode = "";

		generatedCode += addSystemDiagnosticsLogic();
		generatedCode += groupFieldsCode(queryFields, groupBy.length() > 0);

		for (Statement statement : queryFields.getSelectField().getStatements()) {
			if (statement instanceof AggregationStatement) {
				AggregationStatement aggregationField = (AggregationStatement) statement;
				String operation = aggregationField.getOperation();

				if (operation.equalsIgnoreCase("count") && aggregationField.getField() != null && aggregationField.getField().length() > 1) {
					generatedCode += addCountDistinctLogic(aggregationField, groupBy);
				}
				if (operation.equalsIgnoreCase("count") && (aggregationField.getField() == null || aggregationField.getField().length() == 1)) {
					generatedCode += addCountLogic(aggregationField, groupBy);
				}
				if (operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("min")) {
					generatedCode += addMinMaxLogic(aggregationField, groupBy, operation.equalsIgnoreCase("min"));
				}
				if (operation.equalsIgnoreCase("sum") || operation.equalsIgnoreCase("avg")) {
					generatedCode += addSumLogic(aggregationField, groupBy);
				}
			}
		}
		generatedCode += tabs + "return out\n";
		return generatedCode;

	}

	private String addCountLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs = getTabs(level + 1);
		String subTabs = getTabs(level + 2);
		String generatedCode = "";
		generatedCode += tabs + "-- count operation or sub operation for avg\n";
		generatedCode += tabs + "if out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] == nil then\n";
		generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = 0\n";
		generatedCode += tabs + "end\n";

		if (aggregationField.getCondition() != null && aggregationField.getCondition().length() > 0) {
			generatedCode += tabs + "if " + aggregationField.getIsTrue() + " and " + aggregationField.getCondition() + " then\n";
		}
		generatedCode += tabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] + 1\n";

		if (aggregationField.getCondition() != null && aggregationField.getCondition().length() > 0) {
			generatedCode += tabs + "end\n\n";
		}
		return generatedCode;
	}

	private String addCountDistinctLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String generatedCode = "";
		generatedCode += tabs1 + "-- count distinct\n";
		generatedCode += tabs1 + "if out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] == nil then\n";
		generatedCode += tabs2 + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = map()\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "if " + aggregationField.getField() + " then\n";
		generatedCode += tabs1 + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"][" + aggregationField.getField() + "] = true\n\n";
		generatedCode += tabs1 + "end\n";
		return generatedCode;
	}

	private String addMinMaxLogic(AggregationStatement aggregationField, String groupBy, boolean minOperation) {
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String limitValue = minOperation ? Definitions.LuaValues.Min.value : Definitions.LuaValues.Max.value;
		String comparisonOperation = minOperation ? "min" : "max";
		String generatedCode = "";
		generatedCode += tabs1 + "-- min or max operation\n";
		generatedCode += tabs1 + "if out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] == nil then\n";

		generatedCode += tabs2 + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = " + limitValue + "\n";
		generatedCode += tabs1 + "end\n";

		String fieldName = aggregationField.getIsTrue() != null ? aggregationField.getIsTrue() : aggregationField.getField();
		generatedCode += tabs1 + "if " + fieldName;
		if (aggregationField.getCondition() != null && aggregationField.getCondition().length() > 0) {
			generatedCode += " and " + aggregationField.getCondition();
		}
		generatedCode += " then\n";
		generatedCode += tabs2 + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = math." + comparisonOperation + "(out" + groupBy + "[\"" + aggregationField.getAlias() + "\"], " + fieldName + ")\n";
		if (aggregationField.getIsFalse() != null) {
			generatedCode += tabs1 + "elseif " + aggregationField.getIsFalse() + " then\n";
			generatedCode += tabs2 + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = math." + comparisonOperation + "(out" + groupBy + "[\"" + aggregationField.getAlias() + "\"], " + aggregationField.getIsFalse() + ")\n";
		}
		generatedCode += tabs1 + "end\n\n";
		return generatedCode;
	}

	private String addSumLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs = getTabs(level + 1);
		String subTabs = getTabs(level + 2);
		String generatedCode = "";
		generatedCode += tabs + "-- sum operation or sub operation for avg\n";

		if (groupBy.length() > 0) {
			generatedCode += tabs + "if out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] == nil then\n";
			generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = 0\n";
			if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
				generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] = 0\n";
			}
			generatedCode += tabs + "end\n\n";
		}

		String fieldName = aggregationField.getIsTrue() != null ? aggregationField.getIsTrue() : aggregationField.getField();
		generatedCode += tabs + "if " + fieldName;
		if (aggregationField.getCondition() != null && aggregationField.getCondition().length() > 0) {
			generatedCode += " and " + aggregationField.getCondition();
		}
		generatedCode += " then\n";
		generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = (out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] or 0) + " + fieldName + "\n";

		if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
			generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] = (out" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] or 0) + 1\n";
		}

		if (aggregationField.getIsFalse() != null && aggregationField.getIsFalse().length() > 0 && !aggregationField.getOperation().equalsIgnoreCase("avg")) {
			generatedCode += tabs + "elseif " + aggregationField.getIsFalse() + " then\n";
			generatedCode += subTabs + "out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = (out" + groupBy + "[\"" + aggregationField.getAlias() + "\"] or 0) + " + aggregationField.getIsFalse() + "\n";
		}
		generatedCode += tabs + "end\n\n";

		return generatedCode;

	}

}
