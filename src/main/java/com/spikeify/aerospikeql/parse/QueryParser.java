package com.spikeify.aerospikeql.parse;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Created by roman on 16/07/15.

 * Parses all fields of SQL query and stores them in Query fields data structure.
 */
public class QueryParser {

	public static QueryFields parseQuery(String query) throws ParserException {
		Statement stmt;

		try {
			stmt = CCJSqlParserUtil.parse(query);
		} catch (JSQLParserException e) {
			String message = e.getCause().getMessage();
			throw new ParserException(message);
		}

		net.sf.jsqlparser.statement.select.Select selectStatement = (net.sf.jsqlparser.statement.select.Select) stmt;
		PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
		QueryFields queryFields = new QueryFields();

		//parse select
		if (plain.getSelectItems() != null) {
			for (SelectItem expression : plain.getSelectItems()) {
				queryFields.getSelectField().getSelectList().add(expression.toString());
			}
		}

		//parse from
		if (plain.getFromItem() == null) {
			throw new ParserException("Please define FROM statements.");

		} else {
			String fromItem = plain.getFromItem().toString();
			String[] fromFields = fromItem.split("\\.");
			if (fromFields.length == 2) {
				queryFields.setNamespace(fromFields[0]);
				queryFields.setSet(fromFields[1]);
			}
		}

		//parse where
		Expression expression = plain.getWhere();
		if (expression != null) {
			queryFields.setWhereField(expression.toString());
		}

		//parse group by
		if (plain.getGroupByColumnReferences() != null) {
			for (Expression expression2 : plain.getGroupByColumnReferences())
				queryFields.getGroupList().add(expression2.toString());

		}

		//parse having
		expression = plain.getHaving();
		if (expression != null) {
			queryFields.getHavingField().setStatement(expression.toString());
		}

		//parse order by
		if (plain.getOrderByElements() != null) {
			for (OrderByElement exp : plain.getOrderByElements()) {
				queryFields.getOrderFields().getOrderList().add(exp.toString());
			}
		}

		//parse limit
		if (plain.getLimit() != null) {
			Long limit = plain.getLimit().getRowCount();
			queryFields.setLimit(limit.intValue());
		}

		queryFields.postProcess(); //post process fields

		return queryFields;
	}

}

































