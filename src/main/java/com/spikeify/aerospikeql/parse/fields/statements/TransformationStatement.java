package com.spikeify.aerospikeql.parse.fields.statements;

/**
 * Created by roman on 09/08/15.

 * TransformationStatement is a field in select statements that makes a transformation on a field. E.g. select day(timestamp)
 */
public class TransformationStatement implements Statement {

	private final String alias;
	private final String condition;
	private final boolean nested;


	public TransformationStatement(String alias, String condition, boolean nested) {
		this.alias = alias;
		this.condition = condition;
		this.nested = nested;
	}

	@Override
	public String getField() {
		return "";
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public String getOperation() {
		return "";
	}

	public String getCondition() {
		return condition;
	}

	@Override
	public boolean isNested() {
		return nested;
	}


	public static class TransformationFieldBuilder {
		private String alias;
		private String condition;
		private boolean nested;

		public TransformationFieldBuilder setAlias(String alias) {
			this.alias = alias;
			return this;
		}

		public TransformationFieldBuilder setCondition(String condition) {
			this.condition = condition;
			return this;
		}

		public TransformationFieldBuilder setNested(boolean nested) {
			this.nested = nested;
			return this;
		}

		public TransformationStatement createTransformationField() {
			return new TransformationStatement(alias, condition, nested);
		}
	}
}
