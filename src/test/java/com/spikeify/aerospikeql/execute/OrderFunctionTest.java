package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OrderFunctionTest {

	Spikeify sfy;
	AerospikeQlService aerospikeQlService;


	@Before
	public void setUp() throws Exception {
		TestAerospike testAerospike = new TestAerospike();
		sfy = testAerospike.getSfy();
		QueryUtils queryUtils = new QueryUtils(sfy, "udf/");
		aerospikeQlService = new AerospikeQlService(sfy, queryUtils);
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

		int i = numRecords + 1;
		entity = new Entity();
		entity.key = String.valueOf(i);
		entity.value = null;
		entity.value2 = i + 1;
		entity.cluster = null;
		sfy.create(entity).now();
	}

	@Test
	public void testSortOneFieldDesc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by value desc";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		assertEquals(101, resultsList.size());
		Long sequence = 100L;
		for (Map<String, Object> map : resultsList) {
			if (sequence > 0) {
				assertEquals(sequence--, map.get("value"));
			} else {
				assertEquals(null, map.get("value"));
			}
		}
	}

	@Test
	public void testSortOneFieldAsc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by value asc";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		assertEquals(101, resultsList.size());
		Long sequence = 1L;
		for (Map<String, Object> map : resultsList) {
			if (sequence < 101) {
				assertEquals(sequence++, map.get("value"));
			} else {
				assertEquals(null, map.get("value"));
			}
		}
	}

	@Test
	public void testSortTwoFieldAsc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by value asc, cluster";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		assertEquals(101, resultsList.size());
		Long sequence = 1L;
		for (Map<String, Object> map : resultsList) {
			if (sequence < 101) {
				assertEquals(sequence++, map.get("value"));
			} else {
				assertEquals(null, map.get("value"));
			}
		}
	}

	@Test
	public void testSortOneFieldStringAsc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by pk asc";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		List<String> strings = new ArrayList<>();
		for (int i = 1; i < 100 + 2; i++) {
			strings.add(String.valueOf(i));
		}
		Collections.sort(strings);

		assertEquals(101, strings.size());
		assertEquals(101, resultsList.size());

		int counter = 0;
		for (Map<String, Object> map : resultsList) {
			assertEquals(strings.get(counter++), map.get("pk"));
		}
	}

	@Test
	public void testSortOneFieldStringDesc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by pk desc";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		List<String> strings = new ArrayList<>();
		for (int i = 1; i < 100 + 2; i++) {
			strings.add(String.valueOf(i));
		}
		Collections.sort(strings, Collections.reverseOrder());

		assertEquals(101, strings.size());
		assertEquals(101, resultsList.size());

		int counter = 0;
		for (Map<String, Object> map : resultsList) {
			assertEquals(strings.get(counter++), map.get("pk"));
		}
	}

	@Test
	public void testSortTwoFieldStringIntegerDesc() throws Exception {
		createSet(100);
		String query = "select primary_key() as pk," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity " +
						"order by pk desc, value";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		List<String> strings = new ArrayList<>();
		for (int i = 1; i < 100 + 2; i++) {
			strings.add(String.valueOf(i));
		}
		Collections.sort(strings, Collections.reverseOrder());

		assertEquals(101, strings.size());
		assertEquals(101, resultsList.size());

		int counter = 0;
		for (Map<String, Object> map : resultsList) {
			assertEquals(strings.get(counter++), map.get("pk"));
		}
	}

	private class Entity {
		@UserKey
		public String key;

		public Integer value;

		public Integer value2;

		public Integer cluster;

	}


}