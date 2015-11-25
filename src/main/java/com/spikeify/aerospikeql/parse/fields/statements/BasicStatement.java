package com.spikeify.aerospikeql.parse.fields.statements;

/**
 * Created by roman on 09/08/15.

 * Basic field is a field in select statements. E.g. select timestamp
 */
public class BasicStatement implements Statement {

	private final String alias;
	private final String field;
	private final boolean nested;

	public BasicStatement(String alias, String field, boolean nested) {
		this.alias = alias;
		this.field = field;
		this.nested = nested;
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public String getOperation() {
		return "";
	}

	public String getField() {
		return field;
	}

	@Override
	public boolean isNested() {
		return nested;
	}


	public static class BasicFieldBuilder {
		private String alias;
		private String field;
		private boolean nested;

		public BasicFieldBuilder setAlias(String alias) {
			this.alias = alias;
			return this;
		}

		public BasicFieldBuilder setField(String field) {
			this.field = field;
			return this;
		}

		public BasicFieldBuilder setNested(boolean nested) {
			this.nested = nested;
			return this;
		}

		public BasicStatement createBasicField() {
			return new BasicStatement(alias, field, nested);
		}
	}


}
