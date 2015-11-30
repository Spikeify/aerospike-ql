package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

import java.util.Map;

public class MapEntity {
	@UserKey
	public String key;

	public Map<String, String> value1;

	public Map<String, String> value2;
	}