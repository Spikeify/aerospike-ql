package com.spikeify.aerospikeql;

import com.aerospike.client.Host;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Initialization of Aerospike database connection ...
 */
public class TestAerospike {

	private static final String DEFAULT_NAMESPACE = "test";
	private static final String DEFAULT_HOST = "127.0.0.1";
	private static final Integer DEFAULT_PORT = 3000;

	private final Logger log = LoggerFactory.getLogger(TestAerospike.class);


	/**
	 * This method brings Spikefy service up to speed
	 * ... can be called multiple times
	 * ... but will execute only once (and once it should be enough)
	 */
	public Spikeify getSfy() {

		if (SpikeifyService.getClient() == null) {
			log.info("Starting Aerospike initialization...");
			Map<String, Integer> hosts;

			log.info("--== LOCALHOST ==--");
			hosts = new HashMap<>();
			hosts.put(DEFAULT_HOST, DEFAULT_PORT);

			log.info("Aerospike default namespace: " + DEFAULT_NAMESPACE);

			List<Host> hostsData = new ArrayList<>();
			for (Map.Entry<String, Integer> stringIntegerEntry : hosts.entrySet()) {
				hostsData.add(new Host(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()));
			}

			SpikeifyService.globalConfig(DEFAULT_NAMESPACE, hostsData.toArray(new Host[hostsData.size()]));

			log.info("Aerospike configured.");

		}

		return SpikeifyService.sfy();
	}

	public static String getDefaultNamespace() {
		return DEFAULT_NAMESPACE;
	}
}
