package com.spikeify.aerospikeql.execute;

/**
 * Created by roman on 17/08/15.
 *
 * Query runtime diagnostics
 */
public class Diagnostics {

	private final long startTime;
	private final long endTime;
	private final long executionTime;
	private final long rowsRetrieved;
	private final long rowsQueried;
	private final long columnsQueried;

	public Diagnostics(long startTime, long endTime, long executionTime, long rowsRetrieved, long rowsQueried, long columnsQueried) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.executionTime = executionTime;
		this.rowsRetrieved = rowsRetrieved;
		this.rowsQueried = rowsQueried;
		this.columnsQueried = columnsQueried;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public long getRowsRetrieved() {
		return rowsRetrieved;
	}

	public long getRowsQueried() {
		return rowsQueried;
	}

	public long getColumnsQueried() {
		return columnsQueried;
	}
}
