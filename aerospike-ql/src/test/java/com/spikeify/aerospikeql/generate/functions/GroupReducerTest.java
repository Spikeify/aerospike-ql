package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GroupReducerTest {

	@Test
	public void testGroupBy() {
		QueryFields queryFields = new QueryFields();
		queryFields.getGroupList().add("timezone");
		queryFields.getGroupList().add("timestamp");


		GroupReducer groupReducer = GroupReducer.factory(queryFields);
		String generatedCode = groupReducer.code;

		String expectedCode = "\t\tfor k, v in map.pairs(val2) do\n" +
						"\t\t\t-- reduce system diagnostics\n" +
						"\t\t\tif k == \"sys_\" then\n" +
						"\t\t\t\tif not val1[\"sys_\"] then\n" +
						"\t\t\t\t\tval1[\"sys_\"] = map()\n" +
						"\t\t\t\t\tval1[\"sys_\"][\"count\"] = 0\n" +
						"\t\t\t\tend\n" +
						"\t\t\t\tval1[\"sys_\"][\"count\"] = val1[\"sys_\"][\"count\"] + v[\"count\"]\n" +
						"\n" +
						"\t\t\telse\n" +
						"\t\t\t\tval1[k] = v\n" +
						"\t\t\tend\n" +
						"\t\tend\n" +
						"\t\treturn val1\n";

		assertEquals(expectedCode, generatedCode);


	}

	@Test
	public void testGroupByWithCounter() {
		QueryFields queryFields = new QueryFields();
		queryFields.getGroupList().add("timezone");
		queryFields.getGroupList().add("timestamp");
		queryFields.getSelectField().setAggregations(true);

		Statement aggregationStatement = new AggregationStatement.AggregationFieldBuilder("counter", "COUNT").setField("*").createAggregationField();
		queryFields.getSelectField().getStatements().add(aggregationStatement);

		GroupReducer groupReducer = GroupReducer.factory(queryFields);
		String generatedCode = groupReducer.code;

		String expectedCode = "\t\tfor k, v in map.pairs(val2) do\n" +
						"\t\t\t-- reduce system diagnostics\n" +
						"\t\t\tif k == \"sys_\" then\n" +
						"\t\t\t\tif not val1[\"sys_\"] then\n" +
						"\t\t\t\t\tval1[\"sys_\"] = map()\n" +
						"\t\t\t\t\tval1[\"sys_\"][\"count\"] = 0\n" +
						"\t\t\t\tend\n" +
						"\t\t\t\tval1[\"sys_\"][\"count\"] = val1[\"sys_\"][\"count\"] + v[\"count\"]\n" +
						"\n" +
						"\t\t\t-- reduce other fields\n" +
						"\t\t\telseif val1[k] then\n" +
						"\t\t\t\tval1[k][\"counter\"] = val1[k][\"counter\"] + v[\"counter\"]\n" +
						"\t\t\telse\n" +
						"\t\t\t\tval1[k] = map() \n" +
						"\t\t\t\tval1[k][\"timezone\"] = v[\"timezone\"]\n" +
						"\t\t\t\tval1[k][\"timestamp\"] = v[\"timestamp\"]\n" +
						"\t\t\t\tval1[k][\"counter\"] = v[\"counter\"]\n" +
						"\t\t\tend\n" +
						"\t\tend\n" +
						"\t\treturn val1\n";

		assertEquals(expectedCode, generatedCode);
	}

	@Test
	public void testCounter() {
		QueryFields queryFields = new QueryFields();
		queryFields.getSelectField().setAggregations(true);

		Statement aggregationStatement = new AggregationStatement.AggregationFieldBuilder("counter", "COUNT").setField("*").createAggregationField();
		queryFields.getSelectField().getStatements().add(aggregationStatement);

		GroupReducer groupReducer = GroupReducer.factory(queryFields);

		String expectedCode = "\t\tfor k, v in map.pairs(val2) do\n" +
						"\t\t\t-- reduce system diagnostics\n" +
						"\t\t\tif k == \"sys_\" then\n" +
						"\t\t\t\tif not val1[\"sys_\"] then\n" +
						"\t\t\t\t\tval1[\"sys_\"] = map()\n" +
						"\t\t\t\t\tval1[\"sys_\"][\"count\"] = 0\n" +
						"\t\t\t\tend\n" +
						"\t\t\t\tval1[\"sys_\"][\"count\"] = val1[\"sys_\"][\"count\"] + v[\"count\"]\n" +
						"\n" +
						"\t\t\telseif k == \"counter\" then\n" +
						"\t\t\t\tval1[k] = (val1[k] or 0) + v\n" +
						"\t\t\tend\n" +
						"\t\tend\n" +
						"\t\treturn val1\n";
		assertEquals(expectedCode, groupReducer.code);


	}


}