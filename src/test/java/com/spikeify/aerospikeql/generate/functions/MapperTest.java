package com.spikeify.aerospikeql.generate.functions;

import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.QueryParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MapperTest {


	@Test
	public void testSelectBasic() throws Exception {
		String query = "select type, hostAppId from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String actualCode = mapper.code;
		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"type\"] = topRec[\"type\"]\n" +
						"\t\ttuple[\"hostAppId\"] = topRec[\"hostAppId\"]\n" +
						"\t\treturn tuple\n";
		assertEquals(expectedCode, actualCode);

	}


	@Test
	public void testSelectSummation() throws Exception {

		String query = "select SUM(case when type='__store/purchase/finished' AND productId != 'null' then quantity else 0 end) as finished," +
						"SUM(price) as price " +
						"from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"type\"] = topRec[\"type\"]\n" +
						"\t\ttuple[\"productId\"] = topRec[\"productId\"]\n" +
						"\t\ttuple[\"quantity\"] = topRec[\"quantity\"]\n" +
						"\t\ttuple[\"price\"] = topRec[\"price\"]\n" +
						"\t\treturn tuple\n";

		String actualCode = mapper.code;

		assertEquals(expectedCode, actualCode);

	}

	@Test
	public void testSelectTransformations() throws Exception {
		String query = "SELECT UTC_MS_TO_DAY(timestamp) as timestamp," +
						"UTC_MS_TO_DAY(installTime) as installTime," +
						"DATEDIFF(UTC_MS_TO_DAY(timestamp), UTC_MS_TO_DAY(installTime)) as dayOffset " +
						"from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"timestamp\"] = utc_ms_to_day(topRec[\"timestamp\"])\n" +
						"\t\ttuple[\"installTime\"] = utc_ms_to_day(topRec[\"installTime\"])\n" +
						"\t\ttuple[\"dayOffset\"] = datediff(utc_ms_to_day(topRec[\"timestamp\"]) , utc_ms_to_day(topRec[\"installTime\"]))\n" +
						"\t\treturn tuple\n";


		String actualCode = mapper.code;
		assertEquals(expectedCode, actualCode);

	}

	@Test
	public void testSelectMixed() throws Exception {
		String query = "select UTC_MS_TO_HOUR(MAP_RETRIEVE(parameters,'localTimestamp')) as timestamp, " +
						"MIN(UTC_MS_TO_DAY(timestamp)) as reported_time, " +
						"hostAppId, " +
						"hostPartnerId, " +
						"country, " +
						"platform, " +
						"type," +
						"MAP_RETRIEVE(parameters,'productID') as productId, " +
						"MAP_RETRIEVE(parameters,'transactionID') as transactionId," +
						"MAP_RETRIEVE(parameters,'priceLocale') as priceLocale," +
						"SUM(INTEGER(MAP_RETRIEVE(parameters,'quantity'))) as quantity," +
						"SUM(FLOAT(MAP_RETRIEVE(parameters,'price'))) as price," +
						"SUM(FLOAT(MAP_RETRIEVE(parameters,'priceUsd'))) as priceUsd," +
						"COUNT(1) as count\n from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"timestamp\"] = utc_ms_to_hour(map_retrieve(topRec[\"parameters\"] , 'localTimestamp'))\n" +
						"\t\ttuple[\"reported_time_timestamp\"] = utc_ms_to_day(topRec[\"timestamp\"])\n" +
						"\t\ttuple[\"hostAppId\"] = topRec[\"hostAppId\"]\n" +
						"\t\ttuple[\"hostPartnerId\"] = topRec[\"hostPartnerId\"]\n" +
						"\t\ttuple[\"country\"] = topRec[\"country\"]\n" +
						"\t\ttuple[\"platform\"] = topRec[\"platform\"]\n" +
						"\t\ttuple[\"type\"] = topRec[\"type\"]\n" +
						"\t\ttuple[\"productId\"] = map_retrieve(topRec[\"parameters\"] , 'productID')\n" +
						"\t\ttuple[\"transactionId\"] = map_retrieve(topRec[\"parameters\"] , 'transactionID')\n" +
						"\t\ttuple[\"priceLocale\"] = map_retrieve(topRec[\"parameters\"] , 'priceLocale')\n" +
						"\t\ttuple[\"quantity_parameters\"] = integer(map_retrieve(topRec[\"parameters\"] , 'quantity'))\n" +
						"\t\ttuple[\"price_parameters\"] = float(map_retrieve(topRec[\"parameters\"] , 'price'))\n" +
						"\t\ttuple[\"priceUsd_parameters\"] = float(map_retrieve(topRec[\"parameters\"] , 'priceUsd'))\n" +
						"\t\treturn tuple\n";

		String actualCode = mapper.code;

		assertEquals(expectedCode, actualCode);
	}

	@Test
	public void testSelectEquations() throws Exception {
		String query = "select UTC_MS_TO_HOUR(MAP_RETRIEVE(parameters,'localTimestamp')*2333)+1 as timestamp, " +
						"MIN(UTC_MS_TO_DAY(timestamp)+ 100) as reported_time, " +
						"(100 + 20 + (40/ 2))/100 %2 as eq1, " +
						"((value1 + 10) * 200+ 20) - 40 as eq2\n" +
						"from ns.st";
		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"timestamp\"] = utc_ms_to_hour(map_retrieve(topRec[\"parameters\"] , 'localTimestamp')*2333)+1\n" +
						"\t\ttuple[\"reported_time_timestamp\"] = utc_ms_to_day(topRec[\"timestamp\"])+100\n" +
						"\t\ttuple[\"eq1\"] = (100+20+(40 / 2)) / 100 % 2\n" +
						"\t\ttuple[\"eq2\"] = ((topRec[\"value1\"]+10)*200+20) - 40\n" +
						"\t\treturn tuple\n";
		String actualCode = mapper.code;

		assertEquals(expectedCode, actualCode);
	}


	@Test
	public void testSelectDates() throws Exception {
		String query = "select hour('2015-12-30 11:11:11') as dateTime," +
						"datediff('2015-12-30', '2015-12-25') as dd from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"dateTime\"] = hour('2015-12-30 11:11:11')\n" +
						"\t\ttuple[\"dd\"] = datediff('2015-12-30' , '2015-12-25')\n" +
						"\t\treturn tuple\n";
		String actualCode = mapper.code;

		assertEquals(expectedCode, actualCode);
	}


	@Test
	public void testSelectPrimaryKey() throws Exception {
		String query = "select date, PRIMARY_KEY() as id, time as hours from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"date\"] = topRec[\"date\"]\n" +
						"\t\ttuple[\"id\"] = primary_key(topRec)\n" +
						"\t\ttuple[\"hours\"] = topRec[\"time\"]\n" +
						"\t\treturn tuple\n";
		String actualCode = mapper.code;

		assertEquals(expectedCode, actualCode);
	}

	@Test
	public void testRecordFields() throws Exception {
		String query = "select PRIMARY_KEY() as id, EXPIRATION() as expiration, digest() as digest, generation() as gen from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\ttuple[\"id\"] = primary_key(topRec)\n" +
						"\t\ttuple[\"expiration\"] = expiration(topRec)\n" +
						"\t\ttuple[\"digest\"] = digest(topRec)\n" +
						"\t\ttuple[\"gen\"] = generation(topRec)\n" +
						"\t\treturn tuple\n";
		String actualCode = mapper.code;
		assertEquals(expectedCode, actualCode);
	}

	@Test
	public void testSelectAll() throws Exception {
		String query = "select * from ns.st";

		QueryFields queryFields = QueryParser.parseQuery(query);
		Mapper mapper = Mapper.factory(queryFields);

		String expectedCode = "\t\tlocal tuple = map()\n" +
						"\t\tnames = record.bin_names(topRec)\n" +
						"\t\tfor i, key in ipairs(names) do\n" +
						"\t\t\ttuple[key] = topRec[key]\n" +
						"\t\tend\n" +
						"\t\ttuple['pk'] = record.key(topRec)\n" +
						"\t\ttuple['gen'] = record.gen(topRec)\n" +
						"\t\ttuple['ttl'] = record.ttl(topRec)\n" +
						"\t\treturn tuple\n";
		String actualCode = mapper.code;
		System.out.println(actualCode);
		assertEquals(expectedCode, actualCode);
	}


}