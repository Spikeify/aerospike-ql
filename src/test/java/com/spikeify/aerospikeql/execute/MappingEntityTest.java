package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.Entity1;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class MappingEntityTest {

	private Spikeify sfy;
	private AerospikeQlService aerospikeQlService;


	@Before
	public void setUp(){
		TestAerospike testAerospike = new TestAerospike();
		sfy = testAerospike.getSfy();
		aerospikeQlService = new AerospikeQlService(sfy);
		sfy.truncateNamespace(TestAerospike.getDefaultNamespace());
	}

	@After
	public void tearDown() {
		sfy.truncateNamespace(TestAerospike.getDefaultNamespace());
	}

	private void createSet(int numRecords) {
		Entity1 entity;
		for (int i = 1; i < numRecords + 1; i++) {
			entity = new Entity1();
			entity.key = String.valueOf(i);
			entity.value = i;
			entity.value2 = i + 1;
			entity.cluster = i % 4;
			sfy.create(entity).now();
		}

		int i = numRecords + 2;
		entity = new Entity1();
		entity.key = String.valueOf(i);
		entity.value = null;
		entity.value2 = i + 1;
		entity.cluster = null;
		sfy.create(entity).now();
	}

	@Test
	public void testMapEntity() throws Exception {
		createSet(100);
		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".Entity1 order by value2";

		List<Entity1> resultsList = aerospikeQlService.execAdhoc(Entity1.class, query).now();
		assertEquals(101, resultsList.size());
		Integer count = 2;
		for (Entity1 entity : resultsList) {
			if (count < 101) {
				assertEquals(count++, entity.value2);
			}
		}

	}

	@Test
	public void testMapEntityAdditionalField() throws Exception {
		createSet(100);
		String query = "select cluster, " +
						"avg(value) as avgValue from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"group by cluster";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(5, resultsList.size());

	}

	@Test
	public void testDisableReverseEntityMapping() throws Exception {
		createSet(100);
		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".Entity1 order by value2";

		List<Map<String,Object>> resultsList = aerospikeQlService.execAdhoc(query).mapQuery(Entity1.class).now();
		assertEquals(101, resultsList.size());
		long count = 2;
		for (Map<String, Object> entity : resultsList) {
			if (count < 101) {
				assertEquals(count++, entity.get("value2"));
			}
		}

	}
}
