package com.spikeify.aerospikeql.entities;

import com.spikeify.annotations.UserKey;

/**
 * Created by hiphop on 12/2/15.
 */
public class BoolEntity {

	@UserKey
	public String key;

	public Boolean value1;

	public Boolean value2;


}
