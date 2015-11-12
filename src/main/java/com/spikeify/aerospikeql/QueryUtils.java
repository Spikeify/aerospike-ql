package com.spikeify.aerospikeql;

import com.aerospike.client.Language;
import com.aerospike.client.task.RegisterTask;
import com.spikeify.FieldMapper;
import com.spikeify.MapperUtils;
import com.spikeify.Spikeify;
import com.spikeify.aerospikeql.generate.CodeGenerator;
import com.spikeify.aerospikeql.parse.QueryFields;
import com.spikeify.aerospikeql.parse.QueryParser;
import com.spikeify.aerospikeql.parse.QueryParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
		} catch (QueryParserException e) {
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


	public String queryTransformation(Class clazz, String query) {
		log.info("query before transformation: {}", query);
		query = query.trim().replaceAll(" +", " ");


		Map<String, FieldMapper> fieldMappers = new HashMap<String, FieldMapper>() {{
			put("PRIMARY_KEY()", MapperUtils.getUserKeyFieldMapper(clazz));
			put("GENERATION()", MapperUtils.getGenerationFieldMapper(clazz));
			put("TTL()", MapperUtils.getExpirationFieldMapper(clazz));
		}};

		//process select *
		boolean selectAll = false;
		if (query.toLowerCase().contains("select *")) {
			selectAll = true;
			Map<String, String> binMappings = MapperUtils.getBinMappings(clazz);

			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null) {
					binMappings.put(fieldMapper.getValue().getFieldName(), fieldMapper.getValue().getFieldName());
				}
			}
			String bins = join(binMappings.keySet(), ", ");
			//process aerospike special fields
			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null) {
					bins = bins.replaceAll("\\b" + fieldMapper.getValue().getFieldName() + "\\b", fieldMapper.getKey() + " as " + fieldMapper.getValue().getFieldName());
				}
			}

			query = query.replaceAll("(?i)select \\*", "SELECT " + bins);
		}

		Map<String, String> binMappings = MapperUtils.getBinMappings(clazz);
		for (Map.Entry<String, String> entry : binMappings.entrySet()) {
			if (!entry.getValue().equals(entry.getKey())) {
				query = query.replaceAll("\\b" + entry.getValue() + "\\b", entry.getKey());
			}
		}

		//process aerospike special fields
		if (!selectAll) {
			for (Map.Entry<String, FieldMapper> fieldMapper : fieldMappers.entrySet()) {
				if (fieldMapper.getValue() != null && query.contains(fieldMapper.getValue().getFieldName())) {
					query = query.replaceAll("\\b" + fieldMapper.getValue().getFieldName() + "\\b", fieldMapper.getKey() + " as " + fieldMapper.getValue().getFieldName());
				}
			}
		}


		log.info("query after transformation: {}", query);
		return query;
	}

	public String join(Set<String> set, String delimiter) {
		StringBuffer output = new StringBuffer();
		for (String key : set) {
			output.append(key + delimiter);
		}
		return output.substring(0, output.length() - delimiter.length());
	}

	public boolean set(Object object, String fieldName, Object fieldValue) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				if (fieldValue == null) {
					field.set(object, null);
				} else if (field.getType().getName().equals("java.lang.Integer")) {
					field.set(object, new Integer(fieldValue.toString()));
				} else if (field.getType().getName().equals("java.lang.Long")) {
					field.set(object, new Long(fieldValue.toString()));
				} else if (field.getType().getName().equals("java.lang.Double")) {
					field.set(object, new Double(fieldValue.toString()));
				} else if (field.getType().getName().equals("java.lang.String")) {
					field.set(object, fieldValue.toString());
				}
				return true;
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return false;
	}

	public Class<?> findEntity(String entitiesPackageName, String query) throws QueryParserException {
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
