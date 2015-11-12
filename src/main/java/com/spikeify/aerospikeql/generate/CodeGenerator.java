package com.spikeify.aerospikeql.generate;

import com.spikeify.aerospikeql.generate.functions.*;
import com.spikeify.aerospikeql.parse.QueryFields;

import java.util.HashSet;

/**
 * Created by roman on 16/07/15.
 * <p/>
 * CodeGenerator defines a pipeline for generating code
 */
public class CodeGenerator {

	private static String addTransformations(HashSet<String> operations, HashSet<String> addedTransformations) {
		String subCode = "";
		for (String operation : operations)
			if (!addedTransformations.contains(operation)) {
				Transformation transformation = Transformation.factory(operation);
				addedTransformations.add(operation);
				subCode += transformation.getFunction();
			}
		return subCode;
	}

	public static String generateCode(QueryFields queryFields) {
		String code = "";
		String stream = "\treturn stream ";
		TopLevel topLevel = TopLevel.factory();
		code += topLevel.getHeader();

		HashSet<String> addedTransformations = new HashSet<>();
		if (queryFields.getWhereField().length() > 0) {
			Filter filter = Filter.factory(queryFields);
			code += addTransformations(queryFields.getTransformationFunctions(), addedTransformations);
			code += filter.getFunction();
			stream += filter.getStreamOperation();
		}

		Mapper mapper = Mapper.factory(queryFields);
		code += addTransformations(queryFields.getTransformationFunctions(), addedTransformations);
		code += mapper.getFunction();
		stream += mapper.getStreamOperation();

		if (queryFields.getGroupList().size() > 0 || queryFields.getSelectField().isAggregations()) {
			GroupAggregator groupAggregator = GroupAggregator.factory(queryFields);
			code += groupAggregator.getFunction();
			stream += groupAggregator.getStreamOperation();

			GroupReducer groupReducer = GroupReducer.factory(queryFields);
			code += groupReducer.getFunction();
			stream += groupReducer.getStreamOperation();
		}

		stream += "\n";
		code += stream;
		code += topLevel.getFooter();

		return code;

	}
}
