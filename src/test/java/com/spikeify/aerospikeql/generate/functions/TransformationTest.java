package com.spikeify.aerospikeql.generate.functions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransformationTest {


	@Test
	public void testNoFieldTransformation() {
		String funName = "now";
		Transformation transformation = Transformation.factory(funName);
		String expectedCode = "\t\treturn currentTimestamp\n";
		assertEquals(expectedCode, transformation.code);

	}

	@Test
	public void testSingleFieldFloatTransformation() {
		String funName = "float";
		Transformation transformation = Transformation.factory(funName);

		String expectedCode = "\t\t-- floats are not supported in UDF. Value is converted to int.\n" +
						"\t\treturn tonumber(value1)\n";
		assertEquals(expectedCode, transformation.code);
	}

	@Test
	public void testSingleFieldIntegerTransformation() {
		String funName = "integer";
		Transformation transformation = Transformation.factory(funName);

		String expectedCode = "\t\t-- first check for exception then value is rounded down\n" +
						"\t\treturn pcall(math.floor, value1) and math.floor(value1)\n";
		assertEquals(expectedCode, transformation.code);

	}

	@Test
	public void testSingleFieldDayTransformation() {
		String funName = "hour";
		Transformation transformation = Transformation.factory(funName);

		String actualCode = transformation.code;

		String expectedCode = "\t\tif value1 then\n" +
						"\t\tlocal hour=value1:match(\".*(%d+):%d+:%d+.*\")\n" +
						"\t\treturn tonumber(hour)\n" +
						"\t\tend\n" +
						"\t\treturn ''\n";
		assertEquals(expectedCode, actualCode);
	}
}