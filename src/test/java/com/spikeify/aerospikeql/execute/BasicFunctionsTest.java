package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.Entity1;
import com.spikeify.aerospikeql.parse.ParserException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BasicFunctionsTest {

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
	public void testSelectAll() throws ParserException {
		createSet(100);
		String query = "select primary_key() as key, " +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101, resultsList.size());
		for (Map<String, Object> map : resultsList) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectAllAsterisk() throws ParserException {
		createSet(100);
		String query = "select * " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101, resultsList.size());
		for (Map<String, Object> map : resultsList) {
			assertEquals(true, map.size() >= 4); //null values are not included, but ttl and generation are added
		}
	}

	@Test
	public void testSelectLimit() throws ParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"limit 10";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(10, resultsList.size());
		for (Map<String, Object> map : resultsList) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectWhere() throws ParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"where cluster = 3";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(25, resultsList.size());
		for (Map<String, Object> map : resultsList) {
			assertEquals(4, map.size());
		}
	}

	@Test
	public void testSelectWhereNotNull() throws ParserException {
		createSet(100);
		String query = "select primary_key() as key," +
						"value, " +
						"value2, " +
						"cluster " +
						"from " + TestAerospike.getDefaultNamespace() + ".Entity1 " +
						"where cluster != null";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(100, resultsList.size());
		for (Map<String, Object> map : resultsList) {
			assertEquals(4, map.size());
		}
	}

}
