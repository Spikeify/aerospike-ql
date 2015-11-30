package com.spikeify.aerospikeql.parse;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class QueryFieldsTest {

	@Test(expected = ParserException.class)
	public void testPostProcessException() throws Exception {
		String query = "select timezone, timestamp as ts from t group by timezone";
		QueryParser.parseQuery(query); //QueryFields is called post process
	}

	@Test
	public void testPostProcessWithoutException() throws Exception {
		String query = "select timezone, timestamp as ts from t group by timezone, ts";

		QueryFields queryFields = QueryParser.parseQuery(query); //QueryFields is called post process
		assertEquals(queryFields.getGroupList().size(), 2);
		assertEquals(queryFields.getSelectField().getStatements().size(), 2);
	}


}