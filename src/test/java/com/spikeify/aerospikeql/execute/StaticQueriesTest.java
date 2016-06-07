package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.entities.Entity1;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StaticQueriesTest {

	private Spikeify sfy;
	private AerospikeQlService aerospikeQlService;
	private QueryUtils queryUtils;

	@Before
	public void setUp(){
		TestAerospike testAerospike = new TestAerospike();
		sfy = testAerospike.getSfy();
		aerospikeQlService = new AerospikeQlService(sfy);
		queryUtils = new QueryUtils(sfy);
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
	public void createRunStaticQuery(){
		createSet(100);
		String queryName = "countQuery";
		String query = "select count(1) as counter1, count(*) as counter2  from " + TestAerospike.getDefaultNamespace() + ".Entity1";

		queryUtils.addUdf(queryName, query);

		//execute query first time
		List<Map<String, Object>> resultList1 = aerospikeQlService.execStatic(query, queryName).now();
		assertEquals(101L, resultList1.get(0).get("counter1"));
		assertEquals(101L, resultList1.get(0).get("counter2"));

		//execute query second time
		List<Map<String, Object>> resultList2 = aerospikeQlService.execStatic(query, queryName).now();
		assertEquals(101L, resultList2.get(0).get("counter1"));
		assertEquals(101L, resultList2.get(0).get("counter2"));

		queryUtils.removeUdf(queryName);
	}


}
