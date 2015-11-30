package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

public class Entity1 {
		@UserKey
		public String key;

		public Integer value;

		public Integer value2;

		public Integer cluster;
	}