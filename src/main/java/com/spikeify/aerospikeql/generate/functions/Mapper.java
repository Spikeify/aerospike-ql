package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.Definitions;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.statements.BasicStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import com.spikeify.aerospikeql.parse.fields.statements.TransformationStatement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by roman on 16/07/15.
 *
 * Logic for map code in lua script
 */
public class Mapper extends Function {

	private Mapper() {
		functionName = "select";
		nameArg1 = "topRec";
		streamOperation = "map";
		level = 1;
		code = "";
	}

	public static Mapper factory(QueryFields queryFields) {
		Mapper mapper = new Mapper();
		mapper.setFunction(queryFields);
		return mapper;
	}

	private void setFunction(QueryFields queryFields) {

		setSignature1Arg();
		List<String> selectFields = queryFields.getSelectField().getSelectList();
		if (Definitions.isSelectAll(selectFields)) {
			code += addSelectAllLogic();
		} else {
			code += addLogic(queryFields);
		}

	}

	private String addSelectAllLogic() {
		String tabs1 = getTabs(level + 1);
		String tabs2 = getTabs(level + 2);

		String generatedCode = "";
		generatedCode += tabs1 + "local tuple = map()\n";
		generatedCode += tabs1 + "names = record.bin_names(" + nameArg1 + ")\n";
		generatedCode += tabs1 + "for i, key in ipairs(names) do\n";
		generatedCode += tabs2 + "tuple[key] = " + nameArg1 + "[key]\n";
		generatedCode += tabs1 + "end\n";
		generatedCode += tabs1 + "tuple['pk'] = record.key(" + nameArg1 + ")\n";
		generatedCode += tabs1 + "tuple['gen'] = record.gen(" + nameArg1 + ")\n";
		generatedCode += tabs1 + "tuple['ttl'] = record.ttl(" + nameArg1 + ")\n";
		generatedCode += tabs1 + "return tuple\n";
		return generatedCode;
	}

	private String addLogic(QueryFields queryFields) {
		String tabs = getTabs(level + 1);
		String generatedCode = "";

		HashSet<String> alreadyAdded = new HashSet<>(); // do add a field twice in a map function
		List<Statement> statements = queryFields.getSelectField().getStatements();
		HashSet<String> functions = new HashSet<>(); // function code to include in LUA script

		generatedCode += tabs + "local tuple = map()\n";

		for (Statement field : statements) {
			if (field instanceof TransformationStatement) {
				//add a transformation field to map function
				TransformationStatement tf = (TransformationStatement) field;
				String statement = tf.getCondition();

				HashMap<String, String> replaceQuotesMapping = new HashMap<>(); //contains: MATCH1: 'value1', MATCH2: value2. We do not want to split strings in quotes.
				statement = preProcessStatementQuotes(replaceQuotesMapping, statement); //replace strings in quotes with MATCH1, etc. to not split them and set replaceQuotesMapping structure.
				String subCode = super.parseStatement(statement, functions); //convert statements to LUA code
				subCode = postProcessStatementQuotes(replaceQuotesMapping, subCode); //replace MATCH1, etc. with original strings in quotes

				subCode = processField(subCode);
				generatedCode += tabs + "tuple[\"" + field.getAlias() + "\"] = " + subCode + "\n";

			} else if (field instanceof BasicStatement) {
				//add a basic field to map function
				BasicStatement bf = (BasicStatement) field;
				String fieldName = bf.getAlias();

				if (!alreadyAdded.contains(fieldName)) {
					generatedCode += tabs + "tuple[\"" + fieldName + "\"] = " + nameArg1 + "[\"" + bf.getField() + "\"]\n";
					alreadyAdded.add(fieldName);
				}
			}

		}
		queryFields.getTransformationFunctions().addAll(functions);
		generatedCode += tabs + "return tuple\n";
		return generatedCode;
	}

}



