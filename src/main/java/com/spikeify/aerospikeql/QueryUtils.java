package com.spikeify.aerospikeql;

import com.aerospike.client.Language;
import com.aerospike.client.task.RegisterTask;
import com.spikeify.*;
import com.spikeify.aerospikeql.generate.CodeGenerator;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.QueryParser;
import com.spikeify.aerospikeql.parse.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;

public class QueryUtils {

	private static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

	private final String udfFolder;
	private final Spikeify sfy;

	public QueryUtils(Spikeify sfy,
	                  String udfFolder) {
		this.sfy = sfy;
		this.udfFolder = udfFolder;
	}

	public void removeUdf(String udfName) {
		try {
			String clientUdfPath = udfFolder + udfName + ".lua";
			log.info("Deleting local LUA file '{}'", clientUdfPath);
			// delete local UDF file
			File file = new File(clientUdfPath);
			file.delete();
		} catch (Exception e) {
			log.error("Error deleting file ", e);
		}
		unregisterUdf(udfName + ".lua");
	}

	public QueryFields addUdf(String udfName, String query) {
		QueryFields queryFields;
		try {
			queryFields = QueryParser.parseQuery(query);
		} catch (ParserException e) {
			log.error(e.getMessage());
			return null;
		}

		if (queryFields != null) {
			String code = CodeGenerator.generateCode(queryFields);
			log.info("Lua code: {}", code);

			saveUdfFile(udfName, code);
			registerUdf(udfName);
		}
		return queryFields;

	}

	private void registerUdf(String udfName) {
		String clientUdfPath = udfFolder + udfName + ".lua";

		log.info("Registering UDF: {}, {}", udfName, clientUdfPath);
		if (udfName != null) {
			RegisterTask task = sfy.getClient().register(null, clientUdfPath, udfName + ".lua", Language.LUA);
			task.waitTillComplete();
		} else {
			log.error("Missing udfName field");
		}
	}

	private void saveUdfFile(String udfName, String content) {
		// recreate LUA file
		String clientUdfPath = udfFolder + udfName + ".lua";
		log.info("Creating file: {}", clientUdfPath);
		File luaFile = new File(clientUdfPath);
		if (luaFile.getParentFile() != null) {
			luaFile.getParentFile().mkdirs();
		}

		PrintWriter printWriter = null;
		try {

			printWriter = new PrintWriter(luaFile);
			printWriter.println(content);

		} catch (Exception e) {
			log.error("Error saving file: ", e);
		} finally {
			if (printWriter != null) {
				printWriter.flush();
				printWriter.close();
			}
		}

	}

	private void unregisterUdf(String udfName) {
		log.info("Unregistering LUA: {}", udfName);
		// remove UDF - update AerospikeClient version
		sfy.getClient().removeUdf(null, udfName);
	}

	public Class<?> findEntity(String entitiesPackageName, String query) throws ParserException {
		Class<?> clazz;
		QueryFields queryFields = QueryParser.parseQuery(query);
		String setName = queryFields.getSet();
		if (!entitiesPackageName.endsWith(".")) {
			entitiesPackageName = entitiesPackageName + ".";

		}
		try {
			clazz = Class.forName(entitiesPackageName + setName);
		} catch (ClassNotFoundException e) {
			log.error(e.getMessage());
			return null;
		}
		return clazz;
	}
}
