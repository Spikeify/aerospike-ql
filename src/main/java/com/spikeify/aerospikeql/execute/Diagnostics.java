package com.spikeify.aerospikeql.execute;

/**
 * Created by roman on 17/08/15.

 * Query runtime diagnostics
 */
public class Diagnostics {

	private final Long startTime;
	private final Long endTime;
	private final Long executionTime;
	private final Long rowsRetrieved;
	private final Long rowsQueried;
	private final Long columnsQueried;

	public Diagnostics(Long startTime, Long endTime, Long executionTime, Long rowsRetrieved, Long rowsQueried, Long columnsQueried) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.executionTime = executionTime;
		this.rowsRetrieved = rowsRetrieved;
		this.rowsQueried = rowsQueried;
		this.columnsQueried = columnsQueried;
	}

	public Long getStartTime() {
		return startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public Long getExecutionTime() {
		return executionTime;
	}

	public long getRowsRetrieved() {
		return rowsRetrieved;
	}

	public Long getRowsQueried() {
		return rowsQueried;
	}

	public long getColumnsQueried() {
		return columnsQueried;
	}
}
