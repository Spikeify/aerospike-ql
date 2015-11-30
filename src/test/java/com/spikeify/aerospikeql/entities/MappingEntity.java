package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Expires;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;

public class MappingEntity {
		@UserKey
		public String key;

		@Expires
		public Long expiration = 60 * 60 * 24 * 30L;

		@Generation
		public Integer generation;

		public Integer value;

		@BinName("mod")
		public Integer veryLongNameForModulo;
	}