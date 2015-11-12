package com.spikeify.aerospikeql.parse;

import com.spikeify.aerospikeql.parse.fields.SelectField;
import com.spikeify.aerospikeql.parse.fields.statements.Statement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SelectFieldTest {

	@Test
	public void testSetFields() throws Exception {
		//test if fields retain correct order after parsing
		SelectField selectField = new SelectField();

		selectField.getSelectList().add("tz");
		selectField.getAliases().add("tz");

		selectField.getSelectList().add("min(timestamp)");
		selectField.getAliases().add("min_ts");

		selectField.getSelectList().add("min(case when timestamp > 0 then timezone else 0 end)");
		selectField.getAliases().add("min_conditions");

		selectField.getSelectList().add("UTC_MS_TO_DAY(timestamp)");
		selectField.getAliases().add("day_timestamp");

		selectField.getSelectList().add("timestamp");
		selectField.getAliases().add("timestamp");

		selectField.getSelectList().add("(timezone * 10 + 4) + 30");
		selectField.getAliases().add("tz2");

		selectField.getSelectList().add("timezone");
		selectField.getAliases().add("timezone");

		selectField.setFields();

		int index = 0;
		for (Statement statement : selectField.getStatements()) {
			assertEquals(statement.getAlias(), selectField.getAliases().get(index));
			index++;
		}

	}


}