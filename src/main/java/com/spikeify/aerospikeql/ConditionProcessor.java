package com.spikeify.aerospikeql;

import com.spikeify.aerospikeql.generate.functions.Filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by sasa on 18/08/15.
 * <p/>
 * parses runtime condition that is injected instead of condition()
 */
public class ConditionProcessor {

	public String process(String condition) {
		Map<String, String> replaceQuotesMapping = new HashMap<>();
		return condition == null ? "true" : new Builder(condition)
						.preProcessStatementQuotes(replaceQuotesMapping)
						.parseStatement()
						.postProcessStatementQuotes(replaceQuotesMapping)
						.build();
	}

	public static class Builder {

		private Filter fc = Filter.factory(null);
		private String condition;

		public Builder(String condition) {
			this.condition = condition;
		}

		protected Builder preProcessStatementQuotes(Map<String, String> replaceQuotesMapping) {
			this.condition = fc.preProcessStatementQuotes(replaceQuotesMapping, condition);
			return this;
		}

		protected Builder parseStatement() {
			this.condition = fc.parseStatement(condition, new HashSet<String>());
			return this;
		}

		protected Builder postProcessStatementQuotes(Map<String, String> replaceQuotesMapping) {
			this.condition = fc.postProcessStatementQuotes(replaceQuotesMapping, condition);
			return this;
		}

		protected String build() {
			return condition != null ? condition : "true";
		}

	}

}
