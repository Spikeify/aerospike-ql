package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.parse.QueryFields;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterTest {

	@Test
	public void testFilterEqualsString() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 = 'session/start' OR field2 != 'session/end'");

		Filter f = Filter.factory(queryFields);

		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] == 'session/start' or topRec[\"field2\"] ~= 'session/end'";

		assertEquals(codeExpected, codeGenerated);
	}

	@Test
	public void testFilterFieldNotNull() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 != null and field1 >= 10 OR field2 = null and field3 != null");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] ~= nil and topRec[\"field1\"] >= 10 or topRec[\"field2\"] == nil and topRec[\"field3\"] ~= nil";

		assertEquals(codeExpected, codeGenerated);
	}


	@Test
	public void testFilterOperatorNumbers() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 >= 10 OR field2 <= 20 and field3 = 3");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] >= 10 or topRec[\"field2\"] <= 20 and topRec[\"field3\"] == 3";

		assertEquals(codeExpected, codeGenerated);
	}

	@Test
	public void testFilterMixedFilter() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 >= 10 OR field2 < 20 and field3 = 3 and field1 = 'session/start' OR field2 = 'session/end'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return topRec[\"field1\"] >= 10 or topRec[\"field2\"] < 20 and topRec[\"field3\"] == 3 and topRec[\"field1\"] == 'session/start' or topRec[\"field2\"] == 'session/end'";

		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testFilterDoubleEqual() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 = 10 OR field2 = 'session/start'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return topRec[\"field1\"] == 10 or topRec[\"field2\"] == 'session/start'";

		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testFilterStringLike() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 = 1 OR field1 like 'start' OR field2 = 'start' and field5 not like 'end'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return topRec[\"field1\"] == 1 or string.match(topRec[\"field1\"], 'start')  or topRec[\"field2\"] == 'start' and not string.match(topRec[\"field5\"], 'end')";
		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testFilterFieldsOnFields() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 = field2 OR field1 like field8 OR field3 != field4 and field5 not like field7");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] == topRec[\"field2\"] or string.match(topRec[\"field1\"], topRec[\"field8\"])  or topRec[\"field3\"] ~= topRec[\"field4\"] and not string.match(topRec[\"field5\"], topRec[\"field7\"])";
		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testFilterNotEquals() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 != field2 OR field1 not like field8 or field != 'type' and field3 not like 'value4'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] ~= topRec[\"field2\"] or not string.match(topRec[\"field1\"], topRec[\"field8\"])  or topRec[\"field\"] ~= 'type' and not string.match(topRec[\"field3\"], 'value4')";
		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testQueryWithRuntimeCondition() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 != field2 and condition()");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return topRec[\"field1\"] ~= topRec[\"field2\"] and (load('return '.. runTimeCondition, '', 't', {topRec=topRec, string=string}))()";
		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testQueryWithRuntimeCondition2() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1 != '+/%%%&&&' and cOnDiTioN()");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return topRec[\"field1\"] ~= '+/%%%&&&' and (load('return '.. runTimeCondition, '', 't', {topRec=topRec, string=string}))()";
		assertEquals(codeExpected, codeGenerated);
	}

	@Test
	public void testQueryWithRuntimeCondition3() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("cOnDiTioN() and field1 != '+/%%%&&&'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return (load('return '.. runTimeCondition, '', 't', {topRec=topRec, string=string}))() and topRec[\"field1\"] ~= '+/%%%&&&'";
		assertEquals(codeExpected, codeGenerated);
	}

	@Test
	public void testQueryAndOrNameTrick() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("field1!=orField2 and andField=10");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();
		String codeExpected = "return topRec[\"field1\"] ~= topRec[\"orField2\"] and topRec[\"andField\"] == 10";

		assertEquals(codeExpected, codeGenerated);

	}

	@Test
	public void testFilterWithPrimaryKey() {
		QueryFields queryFields = new QueryFields();
		queryFields.setWhereField("Primary_key() = 'aaabbb' or field1 != field2 and Primary_key() = 'aaaaaaa'");

		Filter f = Filter.factory(queryFields);
		String codeGenerated = f.code.trim();

		String codeExpected = "return primary_key(topRec) == 'aaabbb' or topRec[\"field1\"] ~= topRec[\"field2\"] and primary_key(topRec) == 'aaaaaaa'";
		assertEquals(codeExpected, codeGenerated);

	}


}