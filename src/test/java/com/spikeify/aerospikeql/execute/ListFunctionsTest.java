package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.ListEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ListFunctionsTest {

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

	private List<String> generateStringList(String prefix, int i) {
		List<String> stringList = new ArrayList<>();
		stringList.add(prefix + String.valueOf(i));
		for (int j = 100; j < 105; j++) {
			stringList.add(prefix + String.valueOf(j));
		}
		stringList.add(null);
		return stringList;
	}

	private void createSet(int size) {
		ListEntity ListEntity;

		for (int i = 0; i < size; i++) {
			List<String> stringList1 = generateStringList("company", i);
			List<String> stringList2 = generateStringList("person", i);

			ListEntity = new ListEntity();
			ListEntity.key = String.valueOf(i);
			ListEntity.value1 = stringList1;
			ListEntity.value2 = stringList2;
			sfy.create(ListEntity).now();
		}

		List<String> stringList2 = generateStringList("person", size + 1);
		ListEntity = new ListEntity();
		ListEntity.key = String.valueOf(size + 1);
		ListEntity.value1 = null;
		ListEntity.value2 = stringList2;
		sfy.create(ListEntity).now();
	}

	@Test
	public void testListContains1() {
		createSet(100);

		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".ListEntity where list_contains(value1, 'company5')";
		List<ListEntity> resultList = aerospikeQlService.execAdhoc(ListEntity.class, query).now();
		assertEquals(1L, resultList.size());
		assertEquals(true, resultList.get(0).value1.contains("company5"));
	}

	@Test
	public void testListContains2() {
		createSet(100);

		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".ListEntity where list_contains(value1, 'company5') or list_contains(value2, 'person3') order by key";
		List<ListEntity> resultList = aerospikeQlService.execAdhoc(ListEntity.class, query).now();
		assertEquals(2L, resultList.size());
		assertEquals(true, resultList.get(0).value2.contains("person3"));
		assertEquals(true, resultList.get(1).value1.contains("company5"));
	}

	@Test
	public void testListContains3() {
		createSet(100);

		String query = "select list_contains(value1, 'company5') as val1, list_contains(value2, 'person3') as val2 from " + TestAerospike.getDefaultNamespace() + ".ListEntity order by val1 desc, val2 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L, resultList.size());
		assertEquals(true, resultList.get(0).get("val1"));
	}

	@Test
	public void testListRetrieve1() {
		createSet(100);

		String query = "select list_retrieve(value1, 'company5') as val1 from " + TestAerospike.getDefaultNamespace() + ".ListEntity order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L, resultList.size());
		assertEquals("company5", resultList.get(0).get("val1"));
		for (int i = 1; i < resultList.size(); i++) {
			assertEquals(null, resultList.get(i).get("val1"));
		}
	}

	@Test
	public void testListRetrieve2() {
		createSet(100);

		String query = "select list_retrieve(value1, 'company5') as val1 from " + TestAerospike.getDefaultNamespace() + ".ListEntity where list_contains(value1, 'company5') order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(1L, resultList.size());
		assertEquals("company5", resultList.get(0).get("val1"));
	}

	@Test
	public void testListRetrieve3() {
		createSet(100);

		String query = "select list_retrieve(value1, 'user5') as val1 from " + TestAerospike.getDefaultNamespace() + ".ListEntity order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L, resultList.size());
		for (Map<String, Object> resultMap : resultList) {
			assertEquals(null, resultMap.get("val1"));
		}
	}

	@Test
	public void testListMatch1() {
		createSet(100);

		String query = "select * from " + TestAerospike.getDefaultNamespace() + ".ListEntity where list_match(value1, 'ny50')";
		List<ListEntity> resultList = aerospikeQlService.execAdhoc(ListEntity.class, query).now();
		assertEquals(1L, resultList.size());
		assertEquals(true, resultList.get(0).value1.contains("company50"));
	}

	@Test
	public void testGroupByList1() {
		createSet(100);

		String query = "select value1 from " + TestAerospike.getDefaultNamespace() + ".ListEntity group by value1 order by value1";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L, resultList.size());
	}
	
}
