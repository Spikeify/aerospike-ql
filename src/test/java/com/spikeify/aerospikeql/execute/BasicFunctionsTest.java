package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQl;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.parse.QueryParserException;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BasicFunctionsTest {

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

	private class Entity {
		@UserKey
		public String key;

		public Integer value;

		public Integer value2;

		public Integer cluster;

	}

	@Test
	public void testSelectAll() throws QueryParserException {
		createSet(100);
		String query = "select primary_key() as key, " +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(101, resultsMap.getResultsData().size());
		for (Map<String, Object> map : resultsMap.getResultsData()) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectAllAsterisk() throws QueryParserException {
		createSet(100);
		String query = "select * " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(101, resultsMap.getResultsData().size());
		for (Map<String, Object> map : resultsMap.getResultsData()) {
			assertEquals(true, map.size() >= 4); //null values are not included, but ttl and generation are added
		}
	}

	@Test
	public void testSelectLimit() throws QueryParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"limit 10";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(10, resultsMap.getResultsData().size());
		for (Map<String, Object> map : resultsMap.getResultsData()) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectWhere() throws QueryParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"where cluster = 3";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(25, resultsMap.getResultsData().size());
		for (Map<String, Object> map : resultsMap.getResultsData()) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectWhereNotNull() throws QueryParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"where cluster != null";

		ResultsMap resultsMap = aerospikeQl.runAdhocQuery(query).asMap();
		assertEquals(100, resultsMap.getResultsData().size());
		for (Map<String, Object> map : resultsMap.getResultsData()) {
			assertEquals(4, map.size());
		}
	}

}
