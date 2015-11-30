package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

import java.util.List;

public class ListEntity {
		@UserKey
		public String key;

		public List<String> value1;

		public List<String> value2;
	}