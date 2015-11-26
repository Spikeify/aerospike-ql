package com.spikeify.aerospikeql.parse;


import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Expires;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MappingTest {

	QueryUtils queryUtils;
	Spikeify sfy;

	@Before
	public void setUp() throws Exception {
		TestAerospike testAerospike = new TestAerospike();
		sfy = testAerospike.getSfy();
		queryUtils = new QueryUtils(sfy, "udf/");
	}

	@Test
	public void testMapping() throws Exception {
		String query = "select key, expiration, generation, value, veryLongNameForModulo from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		String transformedQuery = queryUtils.queryTransformation(Entity.class, query);
		assertEquals("select PRIMARY_KEY() as key, TTL() as expiration, GENERATION() as generation, value, mod from test.Entity", transformedQuery);


	}

	@Test
	public void testSelectAllMapping() throws Exception {
		String query = "select * from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity where veryLongNameForModulo > 10";
		String transformedQuery = queryUtils.queryTransformation(Entity.class, query);
		assertEquals("SELECT TTL() as expiration, GENERATION() as generation, PRIMARY_KEY() as key, mod, value from test.Entity where mod > 10", transformedQuery);

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

}
