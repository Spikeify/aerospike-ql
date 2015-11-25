package com.spikeify.aerospikeql.generate.functions;

/**
 * Created by roman on 16/07/15.

 * Top level defines main function that wraps all other function in lua script.
 */
public class TopLevel extends Function {

	private TopLevel() {
		functionName = "main";
		nameArg1 = "stream";
		nameArg2 = "currentTimestamp"; //main function arguments are set in QueryExecute class
		nameArg3 = "runTimeCondition";
		level = 0;
		code = "";
	}

	public static TopLevel factory() {
		TopLevel topLevel = new TopLevel();
		topLevel.setFunction();
		return topLevel;
	}

	public void setFunction() {
		setSignature3Arg();
	}

}
