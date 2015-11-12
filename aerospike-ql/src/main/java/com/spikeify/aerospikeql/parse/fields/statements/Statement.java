package com.spikeify.aerospikeql.parse.fields.statements;

/**
 * Created by roman on 09/08/15.
 * <p/>
 * Type Statement is used for parsing select fields. 3 fields implement it: AggregationStatement, TransformationStatement, BasicStatement.
 */
public interface Statement {

	String getField(); //get field name

	String getAlias(); //get field alias

	String getOperation(); //field operation

	boolean isNested();
}
