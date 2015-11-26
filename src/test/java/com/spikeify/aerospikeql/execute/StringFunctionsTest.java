package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQl;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StringFunctionsTest {

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

	private void createSet() {
		Entity entity;
		List<String> stringList = Arrays.asList("String1", "String2", "Abba", "Queens", "Sam Smith", null);

		for (int i = 0; i < stringList.size(); i++) {
			entity = new Entity();
			entity.key = String.valueOf(i);
			entity.value1 = stringList.get(i);
			entity.value2 = String.valueOf(i);
			sfy.create(entity).now();
		}
	}

	public static class Entity {

		public Entity(){}

		@UserKey
		public String key;

		public String value1;

		public String value2;

	}

	@Test
	public void testRegexMatch1(){
		createSet();
		List<Entity> entityList = aerospikeQl.runAdhocQuery("select * from test.Entity where REGEXP_MATCH(value1, '.*bb.*')").asType(Entity.class).getResultsData();
		assertEquals(1L, entityList.size());
		assertEquals("Abba", entityList.get(0).value1);
	}


	@Test
	public void testRegexMatch2(){
		createSet();
		List<Entity> entityList = aerospikeQl.runAdhocQuery("select * from test.Entity where REGEXP_MATCH(value1, '.*bb.*') or REGEXP_MATCH(value1, '.*m S') order by value1").asType(Entity.class).getResultsData();
		assertEquals(2L, entityList.size());
		assertEquals("Abba", entityList.get(0).value1);
		assertEquals("Sam Smith", entityList.get(1).value1);
	}

//	@Test
//	public void testRegexCaseInsensitive(){
//		createSet();
//		List<Entity> entityList = aerospikeQl.runAdhocQuery("select * from test.Entity where REGEXP_MATCH(value1, '.*BB.*')").asType(Entity.class).getResultsData();
//		assertEquals(1L, entityList.size());
//		assertEquals("Abba", entityList.get(0).value1);
//	}





}
