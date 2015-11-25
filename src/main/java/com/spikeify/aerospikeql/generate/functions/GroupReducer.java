package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.common.Definitions;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;

/**
 * Created by roman on 17/07/15.

 * * Logic for reducer code in lua script
 */
public class GroupReducer extends Function {

	private GroupReducer() {
		functionName = "group_reducer";
		nameArg1 = "val1";
		nameArg2 = "val2";
		streamOperation = "reduce";
		level = 1;
	}

	public static GroupReducer factory(QueryFields queryFields) {
		GroupReducer groupReducer = new GroupReducer();
		groupReducer.setFunction(queryFields);
		return groupReducer;
	}

	private void setFunction(QueryFields queryFields) {
		code = "";
		setSignature2Arg();
		if (!queryFields.getSelectField().isAggregations()) {
			code += addLogic();

		} else if (queryFields.getGroupList().size() == 0 && queryFields.getSelectField().isAggregations()) {
			code += addAggregationNoGroupLogic(queryFields);
		} else {
			code += addAggregationLogic(queryFields);
		}
	}

	private String addLogic() {
		//{1432080265766=true, 1432080290913=true...}
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);

		String generatedCode = "";
		generatedCode += tabs1 + "for k, v in map.pairs(val2) do\n";
		generatedCode += addSystemDiagnosticsLogic();
		generatedCode += tabs2 + "else\n";
		generatedCode += tabs3 + "val1[k] = v\n";
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return val1\n";
		return generatedCode;

	}

	private String addSystemDiagnosticsLogic() {
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);
		String tabs4 = getTabs(level + 4);
		String generatedCode = "";
		generatedCode += tabs2 + "-- reduce system diagnostics\n";
		generatedCode += tabs2 + "if k == \"sys_\" then\n";
		generatedCode += tabs3 + "if not val1[\"sys_\"] then\n";
		generatedCode += tabs4 + "val1[\"sys_\"] = map()\n";
		generatedCode += tabs4 + "val1[\"sys_\"][\"count\"] = 0\n";
		generatedCode += tabs3 + "end\n";
		generatedCode += tabs3 + "val1[\"sys_\"][\"count\"] = val1[\"sys_\"][\"count\"] + v[\"count\"]\n\n";
		return generatedCode;
	}


	private String addAggregationLogic(QueryFields queryFields) {
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);
		String tabs3 = getTabs(level + 3);
		String generatedCode = "";

		generatedCode += tabs1 + "for k, v in map.pairs(val2) do\n";
		generatedCode += addSystemDiagnosticsLogic();
		generatedCode += tabs2 + "-- reduce other fields\n";
		generatedCode += tabs2 + "elseif val1[k] then\n";

		for (Statement statement : queryFields.getSelectField().getStatements()) {
			if (statement instanceof AggregationStatement) {
				AggregationStatement aggregationField = (AggregationStatement) statement;
				String operation = aggregationField.getOperation();

				if (operation.equalsIgnoreCase("count") && (aggregationField.getField() == null || aggregationField.getField().length() == 1) || operation.equalsIgnoreCase("sum") || operation.equalsIgnoreCase("avg")) {
					generatedCode += addSummationGroupLogic(aggregationField, "[k]");
				} else if (operation.equalsIgnoreCase("count") && aggregationField.getField().length() > 1) {
					generatedCode += addCountDistinctGroupLogic(aggregationField, "[k]");
				} else if (operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("min")) {
					generatedCode += addMinMaxGroupLogic(aggregationField, "[k]", operation.equalsIgnoreCase("min"));
				}
			}
		}
		generatedCode += tabs2 + "else\n";
		generatedCode += tabs3 + "val1[k] = map() \n";
		for (String field : queryFields.getGroupList()) {
			generatedCode += tabs3 + "val1[k][\"" + field + "\"] = v[\"" + field + "\"]\n";
		}

		for (Statement statement : queryFields.getSelectField().getStatements()) {
			if (statement instanceof AggregationStatement) {
				AggregationStatement aggregationField = (AggregationStatement) statement;
				generatedCode += addInitGroupLogic(aggregationField, "[k]");
			}
		}
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return val1\n";
		return generatedCode;

	}

	private String addAggregationNoGroupLogic(QueryFields queryFields) {
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);

		String generatedCode = "";
		generatedCode += tabs1 + "for k, v in map.pairs(val2) do\n";
		generatedCode += addSystemDiagnosticsLogic();
		for (Statement statement : queryFields.getSelectField().getStatements()) {
			if (statement instanceof AggregationStatement) {
				AggregationStatement aggregationField = (AggregationStatement) statement;
				String operation = aggregationField.getOperation();

				generatedCode += tabs2 + "elseif k == \"" + aggregationField.getAlias() + "\" then\n";

				if (operation.equalsIgnoreCase("count") && (aggregationField.getField() == null || aggregationField.getField().length() == 1) || operation.equalsIgnoreCase("sum") || operation.equalsIgnoreCase("avg")) {
					generatedCode += addSummationLogic(aggregationField);

				} else if (operation.equalsIgnoreCase("count") && aggregationField.getField().length() > 1) {
					generatedCode += addCountDistinctLogic(aggregationField);

				} else if (operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("min")) {
					generatedCode += addMinMaxLogic(operation.equalsIgnoreCase("min"));
				}
			}
		}
		generatedCode += tabs2 + "end\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "return val1\n";
		return generatedCode;

	}


	private String addCountDistinctLogic(AggregationStatement aggregationField) {
		String tabs3 = getTabs(level + 3);
		String tabs4 = getTabs(level + 4);
		String tabs5 = getTabs(level + 5);
		String generatedCode = "";

		generatedCode += tabs3 + "if val1[\"" + aggregationField.getAlias() + "\"] then\n";
		generatedCode += tabs4 + "for sub_k, sub_v in map.pairs(v) do\n";
		generatedCode += tabs5 + "val1[\"" + aggregationField.getAlias() + "\"][sub_k] = sub_v\n";
		generatedCode += tabs4 + "end\n";
		generatedCode += tabs3 + "else\n";
		generatedCode += tabs4 + "val1[\"" + aggregationField.getAlias() + "\"] = v\n";
		generatedCode += tabs3 + "end\n";
		return generatedCode;

	}

	private String addCountDistinctGroupLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs3 = getTabs(level + 3);
		String tabs4 = getTabs(level + 4);
		String generatedCode = "";
		generatedCode += tabs3 + "for sub_k, sub_v in map.pairs(v[\"" + aggregationField.getAlias() + "\"]) do\n";
		generatedCode += tabs4 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "\"][sub_k] = sub_v\n";
		generatedCode += tabs3 + "end\n\n";
		return generatedCode;
	}

	private String addSummationLogic(AggregationStatement aggregationField) {
		String tabs3 = getTabs(level + 3);
		String generatedCode = tabs3 + "val1[k] = (val1[k] or 0) + v\n";
		if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
			generatedCode += tabs3 + "elseif k == \"" + aggregationField.getAlias() + "_count_\" then\n";
			generatedCode += tabs3 + "val1[k] = (val1[k] or 0) + v\n";
		}
		return generatedCode;
	}

	private String addSummationGroupLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs3 = getTabs(level + 3);
		String generatedCode = tabs3 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = val1" + groupBy + "[\"" + aggregationField.getAlias() + "\"] + v[\"" + aggregationField.getAlias() + "\"]\n";
		if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
			generatedCode += tabs3 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] = val1" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] + v[\"" + aggregationField.getAlias() + "_count_\"]\n";
		}
		return generatedCode;
	}

	private String addInitGroupLogic(AggregationStatement aggregationField, String groupBy) {
		String tabs3 = getTabs(level + 3);
		String generatedCode = tabs3 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = v[\"" + aggregationField.getAlias() + "\"]\n";
		if (aggregationField.getOperation().equalsIgnoreCase("avg")) {
			generatedCode += tabs3 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "_count_\"] = v[\"" + aggregationField.getAlias() + "_count_\"]\n";
		}
		return generatedCode;
	}

	private String addMinMaxLogic(boolean isMin) {
		String tabs3 = getTabs(level + 3);
		String limitValue = isMin ? Definitions.LuaValues.Min.value : Definitions.LuaValues.Max.value;
		String operation = isMin ? "min" : "max";
		String generatedCode = tabs3 + "val1[k] = math." + operation + "(val1[k] or " + limitValue + ", v)\n";
		return generatedCode;

	}

	private String addMinMaxGroupLogic(AggregationStatement aggregationField, String groupBy, boolean minOperation) {
		String tabs3 = getTabs(level + 3);
		String comparisonOperation = minOperation ? "min" : "max";
		String generatedCode = tabs3 + "val1" + groupBy + "[\"" + aggregationField.getAlias() + "\"] = math." + comparisonOperation + "(val1 " + groupBy + "[\"" + aggregationField.getAlias() + "\"], v[\"" + aggregationField.getAlias() + "\"])\n";

		return generatedCode;
	}
}
