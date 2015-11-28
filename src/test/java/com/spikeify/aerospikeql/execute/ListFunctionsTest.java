package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQl;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListFunctionsTest {

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

	private List<String> generateStringList(String prefix, int size){
		List<String> stringList = new ArrayList<>();
		for(int i=0; i<size; i++){
			stringList.add(prefix+String.valueOf(i));
		}
		stringList.add(null);
		return stringList;
	}

	private void createSet(int size) {
		Entity entity;
		List<String> stringList1 = generateStringList("company", 5);
		List<String> stringList2 = generateStringList("person", 5);


		for (int i = 0; i < size; i++) {
			entity = new Entity();
			entity.key = String.valueOf(i);
			entity.value1 = stringList1;
			entity.value2 = stringList2;
			sfy.create(entity).now();
		}

		entity = new Entity();
		entity.key = String.valueOf(size+1);
		entity.value1 = stringList1;
		entity.value2 = null;
		sfy.create(entity).now();
	}

	private static class Entity {

		public Entity(){}

		@UserKey
		public String key;

		public List<String> value1;

		public List<String> value2;

	}

	@Test
	public void testListRetrieve1(){

	}



























































}
