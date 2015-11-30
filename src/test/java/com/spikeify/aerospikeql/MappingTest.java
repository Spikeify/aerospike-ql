package com.spikeify.aerospikeql;


import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.entities.MappingEntity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MappingTest {
	private ExecutorAdhoc executorAdhoc;

	@Before
	public void setUp(){
		TestAerospike testAerospike = new TestAerospike();
		Spikeify sfy = testAerospike.getSfy();
		AerospikeQlService aerospikeQlService = new AerospikeQlService(sfy);
		executorAdhoc = (ExecutorAdhoc) aerospikeQlService.execAdhoc("");
	}

	@Test
	public void testMapping() throws Exception {
		String query = "select key, expiration, generation, value, veryLongNameForModulo from " + TestAerospike.getDefaultNamespace() + ".MappingEntity";
		String transformedQuery = executorAdhoc.queryTransformation(MappingEntity.class, query);
		assertEquals("select PRIMARY_KEY() as key, EXPIRATION() as expiration, GENERATION() as generation, value, mod from " + TestAerospike.getDefaultNamespace() +".MappingEntity", transformedQuery);

	}

	@Test
	public void testSelectAllMapping() throws Exception {
		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".MappingEntity where veryLongNameForModulo > 10";
		String transformedQuery = executorAdhoc.queryTransformation(MappingEntity.class, query);
		assertEquals("SELECT EXPIRATION() as expiration, GENERATION() as generation, PRIMARY_KEY() as key, mod, value from " + TestAerospike.getDefaultNamespace() + ".MappingEntity where mod > 10", transformedQuery);

	}



}
