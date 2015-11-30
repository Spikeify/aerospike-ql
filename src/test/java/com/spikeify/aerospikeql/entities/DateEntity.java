package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

public class DateEntity {
		@UserKey
		public String key;
		public Long value;
		public Long timestamp;
		public Long timestamp2;
		public String time1;
		public String date1;
		public String dateTime;
	}