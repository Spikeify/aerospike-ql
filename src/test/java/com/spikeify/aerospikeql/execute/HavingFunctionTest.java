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

public class HavingFunctionTest {

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
	public void testAggregationHavingOneCondition() throws Exception {
		createSet(100);
		String query = "select cluster," +
						"sum(value) as sumValue, " +
						"avg(value) as avgValue, " +
						"min(value) as minValue," +
						"max(value) as maxValue," +
						"count(*) as counter " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"group by cluster " +
						"having avgValue > 50 " +
						"order by cluster asc";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		assertEquals(2, resultsList.size());
		assertEquals(0L, resultsList.get(0).get("cluster"));
		assertEquals(1300L, resultsList.get(0).get("sumValue"));
		assertEquals(52.0, resultsList.get(0).get("avgValue"));
		assertEquals(4L, resultsList.get(0).get("minValue"));
		assertEquals(100L, resultsList.get(0).get("maxValue"));
		assertEquals(25L, resultsList.get(0).get("counter"));

		assertEquals(3L, resultsList.get(1).get("cluster"));
		assertEquals(1275L, resultsList.get(1).get("sumValue"));
		assertEquals(51.0, resultsList.get(1).get("avgValue"));
		assertEquals(3L, resultsList.get(1).get("minValue"));
		assertEquals(99L, resultsList.get(1).get("maxValue"));
		assertEquals(25L, resultsList.get(1).get("counter"));
	}

	@Test
	public void testAggregationHavingTwoCondition() throws Exception {
		createSet(100);
		String query = "select cluster," +
						"sum(value) as sumValue, " +
						"avg(value) as avgValue, " +
						"min(value) as minValue," +
						"max(value) as maxValue," +
						"count(*) as counter " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"group by cluster " +
						"having avgValue > 50 and maxValue = 99";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(1, resultsList.size());
		assertEquals(3L, resultsList.get(0).get("cluster"));
		assertEquals(1275L, resultsList.get(0).get("sumValue"));
		assertEquals(51.0, resultsList.get(0).get("avgValue"));
		assertEquals(3L, resultsList.get(0).get("minValue"));
		assertEquals(99L, resultsList.get(0).get("maxValue"));
		assertEquals(25L, resultsList.get(0).get("counter"));
	}
	

}
