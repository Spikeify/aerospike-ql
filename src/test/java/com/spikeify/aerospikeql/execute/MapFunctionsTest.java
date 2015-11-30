package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.MapEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapFunctionsTest {


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

	private Map<String, String> createMapStringString(String prefix, int start, int size) {
		Map<String, String> map = new HashMap<>();
		for (int i = start; i < (start + size); i++) {
			map.put(prefix + String.valueOf(i), prefix + String.valueOf(i));
		}
		return map;
	}

	private void createSet(int size, int mapSize) {

		for (int i = 0; i < size; i++) {
			MapEntity MapEntity = new MapEntity();
			MapEntity.key = String.valueOf(i);
			MapEntity.value1 = createMapStringString("person", i * mapSize, mapSize);
			MapEntity.value1 = createMapStringString("person", i * mapSize, mapSize);
			sfy.create(MapEntity).now();
		}

		int i = size + 1;
		MapEntity MapEntity = new MapEntity();
		MapEntity.key = String.valueOf(i);
		MapEntity.value1 = null;
		MapEntity.value1 = createMapStringString("person", i * mapSize, mapSize);
		sfy.create(MapEntity).now();


	}

	@Test
	public void testMapContains1() {
		createSet(100, 5);
		List<MapEntity> resultList = aerospikeQlService.execAdhoc(MapEntity.class, "select * from " + TestAerospike.getDefaultNamespace() + ".MapEntity where map_contains(value1, 'person1')").now();
		assertEquals(1L, resultList.size());
		assertEquals(true, resultList.get(0).value1.containsKey("person1"));
	}

	@Test
	public void testMapRetrieve1() {
		createSet(100, 5);
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc("select map_retrieve(value1, 'person1') as val1 from " + TestAerospike.getDefaultNamespace() + ".MapEntity order by val1 desc").now();
		assertEquals(101L, resultList.size());
		assertEquals("person1", resultList.get(0).get("val1"));

		for (int i = 1; i < resultList.size(); i++) {
			assertEquals(null, resultList.get(i).get("val1"));
		}
	}

	@Test
	public void testMapRetrieve2() {
		createSet(100, 5);
		List<Map<String, Object>> resultList = aerospikeQlService.execAdhoc("select map_retrieve(value1, 'person1') as val1 from " + TestAerospike.getDefaultNamespace() + ".MapEntity where map_contains(value1, 'person1')").now();
		assertEquals(1L, resultList.size());
		assertEquals("person1", resultList.get(0).get("val1"));
	}

}
