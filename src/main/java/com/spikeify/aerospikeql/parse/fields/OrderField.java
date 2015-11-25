package com.spikeify.aerospikeql.parse.fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by roman on 22/07/15.

 * this class sets order direction (asc, desc) of fields in ORDER BY statements
 */

public class OrderField {

	private final List<String> orderList;
	private final HashMap<String, Integer> orderDirection;

	public OrderField() {
		this.orderList = new ArrayList<>();
		this.orderDirection = new HashMap<>();

	}

	public List<String> getOrderList() {
		return orderList;
	}


	public void setOrderDirection() {
		int index = 0;
		for (String field : orderList) {
			String[] split = field.split(" ");

			if (split.length == 2) {
				if (split[1].equalsIgnoreCase("DESC")) {
					orderList.set(index, split[0]);
					this.orderDirection.put(orderList.get(index), -1);
				} else {
					orderList.set(index, split[0]);
					this.orderDirection.put(orderList.get(index), 1);
				}

			} else {
				//default ASC
				this.orderDirection.put(orderList.get(index), 1);
			}
			index++;
		}


	}


	public HashMap<String, Integer> getOrderDirection() {
		return orderDirection;
	}


}
