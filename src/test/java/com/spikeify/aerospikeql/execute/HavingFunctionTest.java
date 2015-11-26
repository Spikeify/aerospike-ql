package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQl;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class HavingFunctionTest {

	Spikeify sfy;
	AerospikeQl aerospikeQl;

	@Before
	public void setUp() throws Exception {
		TestAerospike testAerospike = new TestAerospike();
		sfy = testAerospike.getSfy();
		QueryUtils queryUtils = new QueryUtils(sfy, "udf/");
		aerospikeQl = new AerospikeQl(sfy, queryUtils);
		sfy.truncateNamespace(TestAerospike.DEFAULT_NAMESPACE);
	}

	@After
	public void tearDown() {
		sfy.truncateNamespace(TestAerospike.DEFAULT_NAMESPACE);
	}

	private void createSet(int numRecords) {
		Entity entity;
		for (int i = 1; i < numRecords + 1; i++) {
			entity = new Entity();
			entity.key = String.valueOf(i);
			entity.value = i;
			entity.value2 = i + 1;
			entity.cluster = i % 4;
			sfy.create(entity).now();
		}

		int i = numRecords + 2;
		entity = new Entity();
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
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"group by cluster " +
						"having avgValue > 50 " +
						"order by cluster asc";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();

		assertEquals(2, resultsMap.getResultsData().size());
		assertEquals(0L, resultsMap.getResultsData().get(0).get("cluster"));
		assertEquals(1300L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(52.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(4L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(100L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(0).get("counter"));

		assertEquals(3L, resultsMap.getResultsData().get(1).get("cluster"));
		assertEquals(1275L, resultsMap.getResultsData().get(1).get("sumValue"));
		assertEquals(51.0, resultsMap.getResultsData().get(1).get("avgValue"));
		assertEquals(3L, resultsMap.getResultsData().get(1).get("minValue"));
		assertEquals(99L, resultsMap.getResultsData().get(1).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(1).get("counter"));
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
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"group by cluster " +
						"having avgValue > 50 and maxValue = 99";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(1, resultsMap.getResultsData().size());
		assertEquals(3L, resultsMap.getResultsData().get(0).get("cluster"));
		assertEquals(1275L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(51.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(3L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(99L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(0).get("counter"));
	}

	private class Entity {
		@UserKey
		public String key;

		public Integer value;

		public Integer value2;

		public Integer cluster;

	}

}
