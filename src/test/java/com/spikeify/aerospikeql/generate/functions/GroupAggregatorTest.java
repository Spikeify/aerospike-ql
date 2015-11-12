package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.fields.statements.AggregationStatement;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GroupAggregatorTest {

	@Test
	public void testGroupBy() {
		QueryFields queryFields = new QueryFields();
		queryFields.getGroupList().add("timezone");
		queryFields.getGroupList().add("timestamp");

		GroupAggregator groupAggregator = GroupAggregator.factory(queryFields);
		String generatedCode = groupAggregator.code;

		String expectedCode = "\t\t-- system diagnostics\n" +
						"\t\tif out[\"sys_\"] == nil then\n" +
						"\t\t\tout[\"sys_\"] = map()\n" +
						"\t\t\tout[\"sys_\"][\"count\"] = 0\n" +
						"\t\tend\n" +
						"\t\tout[\"sys_\"][\"count\"] = out[\"sys_\"][\"count\"] + 1\n" +
						"\n" +
						"\t\t-- grouping operation\n" +
						"\t\tlocal timezone = topRec[\"timezone\"]\n" +
						"\t\tlocal timestamp = topRec[\"timestamp\"]\n" +
						"\t\tlocal groupBy = tostring(timezone)..\":\"..tostring(timestamp)\n" +
						"\n" +
						"\t\tif out[groupBy] == nil then\n" +
						"\t\t\tout[groupBy] = map()\n" +
						"\t\t\tout[groupBy][\"timezone\"] = timezone\n" +
						"\t\t\tout[groupBy][\"timestamp\"] = timestamp\n" +
						"\t\tend\n" +
						"\t\treturn out\n";

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

		GroupAggregator groupAggregator = GroupAggregator.factory(queryFields);
		String generatedCode = groupAggregator.code;

		String expectedCode = "\t\t-- system diagnostics\n" +
						"\t\tif out[\"sys_\"] == nil then\n" +
						"\t\t\tout[\"sys_\"] = map()\n" +
						"\t\t\tout[\"sys_\"][\"count\"] = 0\n" +
						"\t\tend\n" +
						"\t\tout[\"sys_\"][\"count\"] = out[\"sys_\"][\"count\"] + 1\n" +
						"\n" +
						"\t\t-- grouping operation\n" +
						"\t\tlocal timezone = topRec[\"timezone\"]\n" +
						"\t\tlocal timestamp = topRec[\"timestamp\"]\n" +
						"\t\tlocal groupBy = tostring(timezone)..\":\"..tostring(timestamp)\n" +
						"\n" +
						"\t\tif out[groupBy] == nil then\n" +
						"\t\t\tout[groupBy] = map()\n" +
						"\t\t\tout[groupBy][\"timezone\"] = timezone\n" +
						"\t\t\tout[groupBy][\"timestamp\"] = timestamp\n" +
						"\t\tend\n" +
						"\t\t-- count operation or sub operation for avg\n" +
						"\t\tif out[groupBy][\"counter\"] == nil then\n" +
						"\t\t\tout[groupBy][\"counter\"] = 0\n" +
						"\t\tend\n" +
						"\t\tout[groupBy][\"counter\"] = out[groupBy][\"counter\"] + 1\n" +
						"\t\treturn out\n";

		assertEquals(expectedCode, generatedCode);


	}


	@Test
	public void testCounter() {
		QueryFields queryFields = new QueryFields();
		queryFields.getSelectField().setAggregations(true);

		Statement aggregationStatement = new AggregationStatement.AggregationFieldBuilder("counter", "COUNT").setField("*").createAggregationField();
		queryFields.getSelectField().getStatements().add(aggregationStatement);

		String expectedCode = "\t\t-- system diagnostics\n" +
						"\t\tif out[\"sys_\"] == nil then\n" +
						"\t\t\tout[\"sys_\"] = map()\n" +
						"\t\t\tout[\"sys_\"][\"count\"] = 0\n" +
						"\t\tend\n" +
						"\t\tout[\"sys_\"][\"count\"] = out[\"sys_\"][\"count\"] + 1\n" +
						"\n" +
						"\t\t-- count operation or sub operation for avg\n" +
						"\t\tif out[\"counter\"] == nil then\n" +
						"\t\t\tout[\"counter\"] = 0\n" +
						"\t\tend\n" +
						"\t\tout[\"counter\"] = out[\"counter\"] + 1\n" +
						"\t\treturn out\n";

		GroupAggregator groupAggregator = GroupAggregator.factory(queryFields);
		assertEquals(expectedCode, groupAggregator.code);


	}

}