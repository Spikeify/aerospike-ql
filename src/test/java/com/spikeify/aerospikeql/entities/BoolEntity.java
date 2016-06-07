package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

public class BoolEntity {

	@UserKey
	public String key;

	public Boolean value1;

	public Boolean value2;


}
