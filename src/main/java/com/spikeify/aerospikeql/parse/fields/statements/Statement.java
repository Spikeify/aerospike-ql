package com.spikeify.aerospikeql.parse.fields.statements;

/**
 * Created by roman on 09/08/15.
 *
 * Type Statement is used for parsing select fields. 3 fields implement it: AggregationStatement, TransformationStatement, BasicStatement.
 */
public interface Statement {

	String getAlias(); //get field alias

	boolean isNested();
}
