package com.spikeify.aerospikeql;

import com.spikeify.Spikeify;

public class AerospikeQl {

	private final Spikeify sfy;
	private final QueryUtils queryUtils;

	public AerospikeQl(Spikeify sfy,
	                   QueryUtils queryUtils) {
		this.sfy = sfy;
		this.queryUtils = queryUtils;
	}


	public Query runAdhocQuery(String query) {
		return new QueryAdhoc<>(sfy, queryUtils, query);
	}

	public Query runStaticQuery(String query, String queryName) {
		return new QueryStatic<>(sfy, queryUtils, query, queryName);
	}


}
