package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQl;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AggregateFunctionsTest {

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
	public void testCount() throws Exception {
		createSet(100);
		String query = "select count(1) as counter1, count(*) as counter2  from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(101L, resultsMap.getResultsData().get(0).get("counter1"));
		assertEquals(101L, resultsMap.getResultsData().get(0).get("counter2"));
	}

	@Test
	public void testCountDistinct() throws Exception {
		createSet(100);
		String query = "select count(distinct cluster) as counter from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(4L, resultsMap.getResultsData().get(0).get("counter"));
	}

	@Test
	public void testAggregation() throws Exception {
		createSet(100);
		String query = "select sum(value) as sumValue, " +
						"avg(value) as avgValue, " +
						"min(value) as minValue," +
						"max(value) as maxValue," +
						"count(*) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(true, resultsMap.getResultsData().size() == 1);
		assertEquals(5050L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(50.5, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(1L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(100L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(101L, resultsMap.getResultsData().get(0).get("counter"));
	}

	@Test
	public void testAggregationsHalfCondition() throws Exception {
		createSet(100);
		String query = "select sum(case when value < 50 then value end) as sumValue, " +
						"avg(case when value < 50 then value end) as avgValue, " +
						"min(case when value < 50 then value end) as minValue," +
						"max(case when value < 50 then value end) as maxValue," +
						"count(case when value < 50 then value end) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(true, resultsMap.getResultsData().size() == 1);
		assertEquals(1225L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(1L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(49L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(49L, resultsMap.getResultsData().get(0).get("counter"));
	}

	@Test
	public void testAggregationsCondition() throws Exception {
		createSet(100);
		String query = "select sum(case when value < 50 then value else 0 end) as sumValue, " +
						"avg(case when value < 50 then value else 0 end) as avgValue, " +
						"min(case when value < 50 then value else 0 end) as minValue," +
						"max(case when value < 50 then value else 0 end) as maxValue," +
						"count(case when value < 50 then value else 0 end) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(true, resultsMap.getResultsData().size() == 1);
		assertEquals(1225L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(49L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(49L, resultsMap.getResultsData().get(0).get("counter"));
	}

	@Test
	public void testAggregationGroup() throws Exception {
		createSet(100);
		String query = "select cluster," +
						"sum(value) as sumValue, " +
						"avg(value) as avgValue, " +
						"min(value) as minValue," +
						"max(value) as maxValue," +
						"count(*) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"group by cluster";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();

		assertEquals(0L, resultsMap.getResultsData().get(0).get("cluster"));
		assertEquals(1300L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(52.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(4L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(100L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(0).get("counter"));

		assertEquals(null, resultsMap.getResultsData().get(1).get("cluster"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("sumValue"));
		assertEquals(0.0, resultsMap.getResultsData().get(1).get("avgValue"));
		assertEquals(null, resultsMap.getResultsData().get(1).get("minValue"));
		assertEquals(null, resultsMap.getResultsData().get(1).get("maxValue"));
		assertEquals(1L, resultsMap.getResultsData().get(1).get("counter"));

		assertEquals(1L, resultsMap.getResultsData().get(2).get("cluster"));
		assertEquals(1225L, resultsMap.getResultsData().get(2).get("sumValue"));
		assertEquals(49.0, resultsMap.getResultsData().get(2).get("avgValue"));
		assertEquals(1L, resultsMap.getResultsData().get(2).get("minValue"));
		assertEquals(97L, resultsMap.getResultsData().get(2).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(2).get("counter"));

		assertEquals(2L, resultsMap.getResultsData().get(3).get("cluster"));
		assertEquals(1250L, resultsMap.getResultsData().get(3).get("sumValue"));
		assertEquals(50.0, resultsMap.getResultsData().get(3).get("avgValue"));
		assertEquals(2L, resultsMap.getResultsData().get(3).get("minValue"));
		assertEquals(98L, resultsMap.getResultsData().get(3).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(3).get("counter"));

		assertEquals(3L, resultsMap.getResultsData().get(4).get("cluster"));
		assertEquals(1275L, resultsMap.getResultsData().get(4).get("sumValue"));
		assertEquals(51.0, resultsMap.getResultsData().get(4).get("avgValue"));
		assertEquals(3L, resultsMap.getResultsData().get(4).get("minValue"));
		assertEquals(99L, resultsMap.getResultsData().get(4).get("maxValue"));
		assertEquals(25L, resultsMap.getResultsData().get(4).get("counter"));


	}

	@Test
	public void testAggregationGroupHalfCondition() throws Exception {
		createSet(100);
		String query = "select cluster, " +
						"sum(case when value < 50 then value end) as sumValue, " +
						"avg(case when value < 50 then value end) as avgValue, " +
						"min(case when value < 50 then value end) as minValue," +
						"max(case when value < 50 then value end) as maxValue," +
						"count(case when value < 50 then value end) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"group by cluster";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();

		assertEquals(0L, resultsMap.getResultsData().get(0).get("cluster"));
		assertEquals(312L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(26.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(4L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(48L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(0).get("counter"));

		assertEquals(null, resultsMap.getResultsData().get(1).get("cluster"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("sumValue"));
		assertEquals(0.0, resultsMap.getResultsData().get(1).get("avgValue"));
		assertEquals(null, resultsMap.getResultsData().get(1).get("minValue"));
		assertEquals(null, resultsMap.getResultsData().get(1).get("maxValue"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("counter"));

		assertEquals(1L, resultsMap.getResultsData().get(2).get("cluster"));
		assertEquals(325L, resultsMap.getResultsData().get(2).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(2).get("avgValue"));
		assertEquals(1L, resultsMap.getResultsData().get(2).get("minValue"));
		assertEquals(49L, resultsMap.getResultsData().get(2).get("maxValue"));
		assertEquals(13L, resultsMap.getResultsData().get(2).get("counter"));

		assertEquals(2L, resultsMap.getResultsData().get(3).get("cluster"));
		assertEquals(288L, resultsMap.getResultsData().get(3).get("sumValue"));
		assertEquals(24.0, resultsMap.getResultsData().get(3).get("avgValue"));
		assertEquals(2L, resultsMap.getResultsData().get(3).get("minValue"));
		assertEquals(46L, resultsMap.getResultsData().get(3).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(3).get("counter"));

		assertEquals(3L, resultsMap.getResultsData().get(4).get("cluster"));
		assertEquals(300L, resultsMap.getResultsData().get(4).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(4).get("avgValue"));
		assertEquals(3L, resultsMap.getResultsData().get(4).get("minValue"));
		assertEquals(47L, resultsMap.getResultsData().get(4).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(4).get("counter"));
	}

	@Test
	public void testAggregationGroupCondition() throws Exception {
		createSet(100);
		String query = "select cluster, " +
						"sum(case when value < 50 then value else 0 end) as sumValue, " +
						"avg(case when value < 50 then value else 0 end) as avgValue, " +
						"min(case when value < 50 then value else 0 end) as minValue," +
						"max(case when value < 50 then value else 0 end) as maxValue," +
						"count(case when value < 50 then value else 0 end) as counter " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"group by cluster";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();

		assertEquals(0L, resultsMap.getResultsData().get(0).get("cluster"));
		assertEquals(312L, resultsMap.getResultsData().get(0).get("sumValue"));
		assertEquals(26.0, resultsMap.getResultsData().get(0).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(0).get("minValue"));
		assertEquals(48L, resultsMap.getResultsData().get(0).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(0).get("counter"));

		assertEquals(null, resultsMap.getResultsData().get(1).get("cluster"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("sumValue"));
		assertEquals(0.0, resultsMap.getResultsData().get(1).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("minValue"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("maxValue"));
		assertEquals(0L, resultsMap.getResultsData().get(1).get("counter"));

		assertEquals(1L, resultsMap.getResultsData().get(2).get("cluster"));
		assertEquals(325L, resultsMap.getResultsData().get(2).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(2).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(2).get("minValue"));
		assertEquals(49L, resultsMap.getResultsData().get(2).get("maxValue"));
		assertEquals(13L, resultsMap.getResultsData().get(2).get("counter"));

		assertEquals(2L, resultsMap.getResultsData().get(3).get("cluster"));
		assertEquals(288L, resultsMap.getResultsData().get(3).get("sumValue"));
		assertEquals(24.0, resultsMap.getResultsData().get(3).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(3).get("minValue"));
		assertEquals(46L, resultsMap.getResultsData().get(3).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(3).get("counter"));

		assertEquals(3L, resultsMap.getResultsData().get(4).get("cluster"));
		assertEquals(300L, resultsMap.getResultsData().get(4).get("sumValue"));
		assertEquals(25.0, resultsMap.getResultsData().get(4).get("avgValue"));
		assertEquals(0L, resultsMap.getResultsData().get(4).get("minValue"));
		assertEquals(47L, resultsMap.getResultsData().get(4).get("maxValue"));
		assertEquals(12L, resultsMap.getResultsData().get(4).get("counter"));
	}

	private class Entity {
		@UserKey
		public String key;

		public Integer value;

		public Integer value2;

		public Integer cluster;

	}

}