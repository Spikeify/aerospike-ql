package com.spikeify.aerospikeql;


import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.*;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Expires;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MappingTest {
	ExecutorAdhoc executorAdhoc;

	@Before
	public void setUp() throws Exception {
		TestAerospike testAerospike = new TestAerospike();
		Spikeify sfy = testAerospike.getSfy();
		QueryUtils queryUtils = new QueryUtils(sfy, "udf/");
		AerospikeQlService aerospikeQlService = new AerospikeQlService(sfy, queryUtils);
		executorAdhoc = (ExecutorAdhoc) aerospikeQlService.execAdhoc("");
	}

	private class Entity {
		@UserKey
		public String key;

		@Expires
		public Long expiration = 60 * 60 * 24 * 30L;

		@Generation
		public Integer generation;

		public Integer value;

		@BinName("mod")
		public Integer veryLongNameForModulo;
	}

	@Test
	public void testMapping() throws Exception {
		String query = "select key, expiration, generation, value, veryLongNameForModulo from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		String transformedQuery = executorAdhoc.queryTransformation(Entity.class, query);
		assertEquals("select PRIMARY_KEY() as key, EXPIRATION() as expiration, GENERATION() as generation, value, mod from test.Entity", transformedQuery);

	}

	@Test
	public void testSelectAllMapping() throws Exception {
		String query = "select * from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity where veryLongNameForModulo > 10";
		String transformedQuery = executorAdhoc.queryTransformation(Entity.class, query);
		assertEquals("SELECT EXPIRATION() as expiration, GENERATION() as generation, PRIMARY_KEY() as key, mod, value from test.Entity where mod > 10", transformedQuery);

	}

}
