package com.spikeify.aerospikeql.parse.fields;

import com.udojava.evalex.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by roman on 07/08/15.

 * HavingField is used to parse conditions of having field
 */
public class HavingField {

	private final ArrayList<String> fields;
	private String statement;
	private Expression expression;

	public HavingField() {
		this.statement = "";
		this.fields = new ArrayList<>();
		this.expression = null;

	}

	public ArrayList<String> getFields() {
		return fields;
	}

	/**
	 * set fields that are in having statements
	 *
	 * @param aliases - list of aliases
	 */

	public void setFields(List<String> aliases) {
		statement = statement.replace("AND", "&&").replace("OR", "||"); //JSQL parser automatically converts and to AND.

		for (String field : aliases) {
			if (statement.contains(field)) {
				fields.add(field);
			}
		}
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Long currentTimestamp) {
		statement = statement.replaceAll("(?i)NOW\\(\\)", String.valueOf(currentTimestamp));
		expression = new Expression(statement);
	}

	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}
}
