package com.spikeify.aerospikeql.execute;

import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.AerospikeQlService;
import com.spikeify.aerospikeql.QueryUtils;
import com.spikeify.aerospikeql.TestAerospike;
import com.spikeify.aerospikeql.parse.ParserException;
import com.spikeify.annotations.UserKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DateFunctionsTest {

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
			entity.timestamp = 1446109765011L;
			entity.timestamp2 = 1445674654000L;
			entity.value = (long) i;
			entity.date1 = "2015-10-29";
			entity.time1 = "09:57:26";
			entity.dateTime = "2015-10-29 09:57:26";


			sfy.create(entity).now();
		}

		int i = numRecords + 2;
		entity = new Entity();
		entity.key = String.valueOf(i);
		entity.timestamp = null;
		entity.timestamp2 = null;
		entity.value = (long) i;
		entity.date1 = null;
		entity.time1 = null;
		entity.dateTime = null;
		sfy.create(entity).now();
	}

	@Test
	public void testCurrentTime() throws ParserException {
		createSet(5);
		Long currentTime = 1446109765000L;

		String query = "select current_time() as currentTime from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).setCurrentTime(currentTime).now();

		DateTime dateTime = new DateTime(currentTime).toDateTime(DateTimeZone.UTC);
		String expectedTime = String.format("%02d:%02d:%02d", dateTime.getHourOfDay(), dateTime.getMinuteOfHour(), dateTime.getSecondOfMinute());

		for (Map<String, Object> entry : resultsList) {
			assertEquals(expectedTime, entry.get("currentTime"));
		}
	}

	@Test
	public void testCurrentDate() throws ParserException {
		createSet(5);

		Long currentTime = 1446109765000L;
		String query = "select current_date() as currentDate from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).setCurrentTime(currentTime).now();

		DateTime dateTime = new DateTime(currentTime).toDateTime(DateTimeZone.UTC);
		String expectedDate = String.format("%d-%02d-%02d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth());

		for (Map<String, Object> entry : resultsList) {
			assertEquals(expectedDate, entry.get("currentDate"));
		}
	}

	@Test
	public void testCurrentTimestamp() throws ParserException {
		createSet(5);

		Long currentTime = 1446109765000L;
		String query = "select current_timestamp() as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).setCurrentTime(currentTime).now();

		DateTime dateTime = new DateTime(currentTime).toDateTime(DateTimeZone.UTC);
		String expectedDate = String.format("%d-%02d-%02d %02d:%02d:%02d UTC", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), dateTime.getHourOfDay(), dateTime.getMinuteOfHour(), dateTime.getSecondOfMinute());

		for (Map<String, Object> entry : resultsList) {
			assertEquals(expectedDate, entry.get("timestamp"));
		}
	}

	@Test
	public void testNow() throws ParserException {
		createSet(5);

		Long currentTime = 1446109765000L;
		String query = "select now() as now from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";

		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).setCurrentTime(currentTime).now();

		for (Map<String, Object> entry : resultsList) {
			assertEquals(currentTime, entry.get("now"));
		}
	}

	@Test
	public void testUTC_MS_TO_SECOND() throws ParserException {
		createSet(5);

		String query = "select UTC_MS_TO_SECOND(timestamp) as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expected = 1446109765000L;
		for (Map<String, Object> entry : resultsList) {
			if ((long) entry.get("timestamp") != 0L) {
				assertEquals(expected, entry.get("timestamp"));
			}
		}
	}

	@Test
	public void testUTC_MS_TO_MINUTE() throws ParserException {
		createSet(5);

		String query = "select UTC_MS_TO_MINUTE(timestamp) as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expected = 1446109740000L;
		for (Map<String, Object> entry : resultsList) {
			if ((long) entry.get("timestamp") != 0L) {
				assertEquals(expected, entry.get("timestamp"));
			}
		}
	}

	@Test
	public void testUTC_MS_TO_HOUR() throws ParserException {
		createSet(5);

		String query = "select UTC_MS_TO_HOUR(timestamp) as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expected = 1446109200000L;
		for (Map<String, Object> entry : resultsList) {
			if ((long) entry.get("timestamp") != 0L) {
				assertEquals(expected, entry.get("timestamp"));
			}
		}

	}

	@Test
	public void testUTC_MS_TO_DAY() throws ParserException {
		createSet(5);

		String query = "select UTC_MS_TO_DAY(timestamp) as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expected = 1446076800000L;
		for (Map<String, Object> entry : resultsList) {
			if ((long) entry.get("timestamp") != 0L) {
				assertEquals(expected, entry.get("timestamp"));
			}
		}

	}

	@Test
	public void testDateConverters() throws ParserException {
		createSet(5);

		String query = "select second('2015-10-29 09:57:26 UTC') as seconds1, " +
						"second('09:57:26 UTC') as seconds2," +
						"minute('09:57:26 UTC') as minutes1, " +
						"minute('2015-10-29 09:57:26 UTC') as minutes2, " +
						"hour('2015-10-29 09:57:26 UTC') as hours1, " +
						"hour('09:57:26 UTC') as hours2," +
						"day('2015-10-29 09:57:26 UTC') as days1, " +
						"day('2015-10-29') as days2, " +
						"month('2015-10-29 09:57:26 UTC') as months1, " +
						"month('2015-10-29') as months2, " +
						"year('2015-10-29 09:57:26 UTC') as years1, " +
						"year('2015-10-29') as years2 " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expectedSeconds = 26L;
		Long expectedMinutes = 57L;
		Long expectedHours = 9L;
		Long expectedDay = 29L;
		Long expectedMonth = 10L;
		Long expectedYear = 2015L;
		for (Map<String, Object> entry : resultsList) {
			assertEquals(expectedSeconds, entry.get("seconds1"));
			assertEquals(expectedSeconds, entry.get("seconds2"));
			assertEquals(expectedMinutes, entry.get("minutes1"));
			assertEquals(expectedMinutes, entry.get("minutes2"));
			assertEquals(expectedHours, entry.get("hours1"));
			assertEquals(expectedHours, entry.get("hours2"));
			assertEquals(expectedDay, entry.get("days1"));
			assertEquals(expectedDay, entry.get("days2"));
			assertEquals(expectedMonth, entry.get("months1"));
			assertEquals(expectedMonth, entry.get("months2"));
			assertEquals(expectedYear, entry.get("years1"));
			assertEquals(expectedYear, entry.get("years2"));
		}
	}

	@Test
	public void testDateConvertersEntity() throws ParserException {
		createSet(5);

		String query = "select second(dateTime) as seconds1, " +
						"second(time1) as seconds2," +
						"minute(dateTime) as minutes1, " +
						"minute(time1) as minutes2, " +
						"hour(dateTime) as hours1, " +
						"hour(time1) as hours2," +
						"day(dateTime) as days1, " +
						"day(date1) as days2, " +
						"month(dateTime) as months1, " +
						"month(date1) as months2, " +
						"year(dateTime) as years1, " +
						"year(date1) as years2 " +
						"from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		Long expectedSeconds = 26L;
		Long expectedMinutes = 57L;
		Long expectedHours = 9L;
		Long expectedDay = 29L;
		Long expectedMonth = 10L;
		Long expectedYear = 2015L;
		for (Map<String, Object> entry : resultsList) {
			if (!entry.get("seconds1").equals("")) {
				assertEquals(expectedSeconds, entry.get("seconds1"));
				assertEquals(expectedSeconds, entry.get("seconds2"));
				assertEquals(expectedMinutes, entry.get("minutes1"));
				assertEquals(expectedMinutes, entry.get("minutes2"));
				assertEquals(expectedHours, entry.get("hours1"));
				assertEquals(expectedHours, entry.get("hours2"));
				assertEquals(expectedDay, entry.get("days1"));
				assertEquals(expectedDay, entry.get("days2"));
				assertEquals(expectedMonth, entry.get("months1"));
				assertEquals(expectedMonth, entry.get("months2"));
				assertEquals(expectedYear, entry.get("years1"));
				assertEquals(expectedYear, entry.get("years2"));
			}
		}
	}

	@Test
	public void testDateDiffMS() throws ParserException {
		createSet(5);

		String query = "select datediff_ms(timestamp, timestamp2) as datediff from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();
		for (Map<String, Object> entry : resultsList) {
			if (entry.get("datediff") != null) {
				assertEquals(5L, entry.get("datediff"));
			}
		}

	}

	@Test
	public void testMSEC_TO_TIMESTAMP() throws ParserException {
		createSet(5);
		String query = "select msec_to_timestamp(timestamp) as timestamp from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

		for (Map<String, Object> entry : resultsList) {
			if (entry.get("timestamp") != null) {
				assertEquals("2015-10-29 09:09:25", entry.get("timestamp"));
			}
		}

	}

	@Test
	public void testDate() throws ParserException {
		createSet(5);
		String query = "select date(dateTime) as date from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();


		for (Map<String, Object> entry : resultsList) {
			if (!entry.get("date").equals("")) {
				assertEquals("2015-10-29", entry.get("date"));
			}
		}

	}

	@Test
	public void testTime() throws ParserException {
		createSet(5);
		String query = "select time(dateTime) as time from " + TestAerospike.DEFAULT_NAMESPACE + ".Entity";
		List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();


		for (Map<String, Object> entry : resultsList) {
			if (!entry.get("time").equals("")) {
				assertEquals("09:57:26", entry.get("time"));
			}
		}

	}

	private class Entity {
		@UserKey
		public String key;
		public Long value;
		public Long timestamp;
		public Long timestamp2;
		public String time1;
		public String date1;
		public String dateTime;
	}


}
