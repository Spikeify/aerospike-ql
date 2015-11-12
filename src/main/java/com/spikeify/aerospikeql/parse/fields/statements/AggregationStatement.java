package com.spikeify.aerospikeql.parse.fields.statements;

/**
 * Created by roman on 03/08/15.
 * <p/>
 * AggregationStatement is a field in select statements that aggregates a field. E.g. select sum(timestamp)
 */
public class AggregationStatement implements Statement {

	private final String alias;
	private final String operation;
	private final String field;
	private final String condition;
	private final String isTrue;
	private final String isFalse;

	public AggregationStatement(String alias, String operation, String condition, String isTrue, String isFalse, String field) {
		this.alias = alias;
		this.operation = operation;
		this.condition = condition == null ? null : condition.replace("AND", "and").replace("OR", "or").replaceAll("(?i)NULL", "nil");
		this.isTrue = isTrue;
		this.isFalse = isFalse;
		this.field = field;
	}

	public String getAlias() {
		return alias;
	}

	public String getOperation() {
		return operation;
	}

	public String getCondition() {
		return condition;
	}

	public String getIsTrue() {
		return isTrue;
	}

	public String getIsFalse() {
		return isFalse;
	}

	public String getField() {
		return field;
	}

	public boolean isNested() {
		return false;
	}

	public static class AggregationFieldBuilder {
		private final String alias;
		private final String operation;
		private String condition;
		private String isTrue;
		private String isFalse;
		private String field;

		public AggregationFieldBuilder(String alias, String operation) {
			this.alias = alias;
			this.operation = operation;
		}

		public AggregationFieldBuilder setCondition(String condition) {
			this.condition = condition;
			return this;
		}

		public AggregationFieldBuilder setIsTrue(String isTrue) {
			this.isTrue = isTrue;
			return this;
		}

		public AggregationFieldBuilder setIsFalse(String isFalse) {
			this.isFalse = isFalse;
			return this;
		}


		public AggregationFieldBuilder setField(String field) {
			this.field = field;
			return this;
		}

		public AggregationStatement createAggregationField() {
			return new AggregationStatement(alias, operation, condition, isTrue, isFalse, field);
		}

	}
}
