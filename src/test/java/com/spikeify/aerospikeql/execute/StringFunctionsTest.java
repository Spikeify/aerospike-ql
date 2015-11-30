package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.StringEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StringFunctionsTest {

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

	private void createSet() {
		StringEntity StringEntity;
		List<String> stringList = Arrays.asList("String1", "String2", "Abba", "Queens", "Sam Smith", null);

		for (int i = 0; i < stringList.size(); i++) {
			StringEntity = new StringEntity();
			StringEntity.key = String.valueOf(i);
			StringEntity.value1 = stringList.get(i);
			StringEntity.value2 = String.valueOf(i);
			sfy.create(StringEntity).now();
		}
	}

	@Test
	public void testRegexMatch1() {
		createSet();
		List<StringEntity> StringEntityList = aerospikeQlService.execAdhoc(StringEntity.class, "select * from " + TestAerospike.getDefaultNamespace() + ".StringEntity where REGEXP_MATCH(value1, '.*bb.*')").now();
		assertEquals(1L, StringEntityList.size());
		assertEquals("Abba", StringEntityList.get(0).value1);
	}

	@Test
	public void testRegexMatch2() {
		createSet();
		List<StringEntity> StringEntityList = aerospikeQlService.execAdhoc(StringEntity.class, "select * from " + TestAerospike.getDefaultNamespace() + ".StringEntity where REGEXP_MATCH(value1, '.*bb.*') or REGEXP_MATCH(value1, '.*m S') order by value1").now();
		assertEquals(2L, StringEntityList.size());
		assertEquals("Abba", StringEntityList.get(0).value1);
		assertEquals("Sam Smith", StringEntityList.get(1).value1);
	}

	@Test
	public void testStringContains1() {
		createSet();
		List<StringEntity> StringEntityList = aerospikeQlService.execAdhoc(StringEntity.class, "select * from " + TestAerospike.getDefaultNamespace() + ".StringEntity where STRING_CONTAINS(value1, 'bb') order by value1").now();
		assertEquals(1L, StringEntityList.size());
		assertEquals("Abba", StringEntityList.get(0).value1);
	}

//	@Test
//	public void testRegexCaseInsensitive(){
//		createSet();
//		List<StringEntity> StringEntityList = aerospikeQlService.execAdhoc("select * from " + TestAerospike.getDefaultNamespace() + ".StringEntity where REGEXP_MATCH(value1, '.*BB.*')").now(StringEntity.class).;
//		assertEquals(1L, StringEntityList.size());
//		assertEquals("Abba", StringEntityList.get(0).value1);
//	}

	@Test
	public void testStringContains2() {
		createSet();
		List<StringEntity> StringEntityList = aerospikeQlService.execAdhoc(StringEntity.class, "select * from " + TestAerospike.getDefaultNamespace() + ".StringEntity where STRING_CONTAINS(value1, 'ee') order by value1").now();
		assertEquals(1L, StringEntityList.size());
		assertEquals("Queens", StringEntityList.get(0).value1);
	}

	@Test
	public void testStringMatch1() {
		createSet();
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc("select STRING_RETRIEVE(value1, 'Abba') as value1 from " + TestAerospike.getDefaultNamespace() + ".StringEntity order by value1 desc").now();
		assertEquals(6L, resultsList.size());
		assertEquals("Abba", resultsList.get(0).get("value1"));
		for (int i = 1; i < resultsList.size(); i++) {
			assertEquals(null, resultsList.get(i).get("value1"));
		}
	}




}
