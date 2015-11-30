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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ListFunctionsTest {

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

	private List<String> generateStringList(String prefix, int i){
		List<String> stringList = new ArrayList<>();
		stringList.add(prefix+String.valueOf(i));
		for(int j=100; j<105; j++){
			stringList.add(prefix+String.valueOf(j));
		}
		stringList.add(null);
		return stringList;
	}

	private List<String> generateStringListWith(String prefix, int size){
		List<String> stringList = new ArrayList<>();
		for(int j=0; j<size; j++){
			stringList.add(prefix+String.valueOf(j));
		}
		return stringList;
	}

	private void createSet(int size) {
		Entity entity;

		for (int i = 0; i < size; i++) {
			List<String> stringList1 = generateStringList("company", i);
			List<String> stringList2 = generateStringList("person", i);

			entity = new Entity();
			entity.key = String.valueOf(i);
			entity.value1 = stringList1;
			entity.value2 = stringList2;
			sfy.create(entity).now();
		}

		List<String> stringList2 = generateStringList("person", size+1);
		entity = new Entity();
		entity.key = String.valueOf(size+1);
		entity.value1 = null;
		entity.value2 = stringList2;
		sfy.create(entity).now();
	}

	private void createSet(int size, int listSize) {
		Entity entity;

		for (int i = 0; i < size; i++) {
			List<String> stringList1 = generateStringListWith("company", listSize);
			List<String> stringList2 = generateStringListWith("person", listSize);

			entity = new Entity();
			entity.key = String.valueOf(i);
			entity.value1 = stringList1;
			entity.value2 = stringList2;
			sfy.create(entity).now();
		}

		List<String> stringList2 = generateStringListWith("person", listSize);
		entity = new Entity();
		entity.key = String.valueOf(size+1);
		entity.value1 = null;
		entity.value2 = stringList2;
		sfy.create(entity).now();
	}

	public static class Entity {

		@UserKey
		public String key;

		public List<String> value1;

		public List<String> value2;

	}

	@Test
	public void testListContains1(){
		createSet(100);

		String query = "select * from test.Entity where list_contains(value1, 'company5')";
		List<Entity> resultList = aerospikeQlService.execAdhoc(Entity.class, query).now();
		assertEquals(1L,resultList.size());
		assertEquals(true, resultList.get(0).value1.contains("company5"));
	}

	@Test
	public void testListContains2(){
		createSet(100);

		String query = "select * from test.Entity where list_contains(value1, 'company5') or list_contains(value2, 'person3') order by key";
		List<Entity> resultList = aerospikeQlService.execAdhoc(Entity.class, query).now();
		assertEquals(2L,resultList.size());
		assertEquals(true, resultList.get(0).value2.contains("person3"));
		assertEquals(true, resultList.get(1).value1.contains("company5"));
	}

	@Test
	public void testListContains3(){
		createSet(100);

		String query = "select list_contains(value1, 'company5') as val1, list_contains(value2, 'person3') as val2 from test.Entity order by val1 desc, val2 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L,resultList.size());
		assertEquals(true, resultList.get(0).get("val1"));
	}

	@Test
	public void testListRetrieve1(){
		createSet(100);

		String query = "select list_retrieve(value1, 'company5') as val1 from test.Entity order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L,resultList.size());
		assertEquals("company5", resultList.get(0).get("val1"));
		for(int i=1; i<resultList.size(); i++){
			assertEquals(null, resultList.get(i).get("val1"));
		}
	}

	@Test
	public void testListRetrieve2(){
		createSet(100);

		String query = "select list_retrieve(value1, 'company5') as val1 from test.Entity where list_contains(value1, 'company5') order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(1L,resultList.size());
		assertEquals("company5", resultList.get(0).get("val1"));
	}


	@Test
	public void testListRetrieve3(){
		createSet(100);

		String query = "select list_retrieve(value1, 'user5') as val1 from test.Entity order by val1 desc";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(101L,resultList.size());
		for(int i=0; i<resultList.size(); i++){
			assertEquals(null, resultList.get(i).get("val1"));
		}
	}

	@Test
	public void testListMatch1(){
		createSet(100);

		String query = "select * from test.Entity where list_match(value1, 'ny50')";
		List<Entity> resultList = aerospikeQlService.execAdhoc(Entity.class, query).now();
		assertEquals(1L,resultList.size());
		assertEquals(true, resultList.get(0).value1.contains("company50"));
	}


	@Test
	public void testGroupByList1(){
		createSet(100, 1);

		String query = "select value1 from test.Entity group by value1 order by value1";
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc(query).now();
		assertEquals(2L,resultList.size());
	}



}
