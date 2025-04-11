/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.antlr.listener;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.text.ParsingException;
import org.devlive.connector.dameng.DamengValueConverters;
import org.devlive.connector.dameng.antlr.OracleDmlParser;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerColumnValueImpl;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerColumnValueWrapper;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.devlive.connector.dameng.antlr.listener.ParserUtils.getTableName;

/**
 * This class contains common methods for DML parser listeners
 */
abstract class BaseDmlParserListener<T>
        extends PlSqlParserBaseListener
{
    protected String catalogName;
    protected String schemaName;
    protected Table table;
    protected OracleDmlParser parser;
    final DamengValueConverters converter;
    String alias;
    Map<T, LogMinerColumnValueWrapper> newColumnValues = new LinkedHashMap<>();
    Map<T, LogMinerColumnValueWrapper> oldColumnValues = new LinkedHashMap<>();

    BaseDmlParserListener(String catalogName, String schemaName, OracleDmlParser parser)
    {
        this.parser = parser;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = parser.getConverters();
    }

    // Defines the key of the Map of LogMinerColumnValueWrapper. It could be String or Integer
    protected abstract T getKey(Column column, int index);

    /**
     * This method prepares all column value placeholders, based on the table metadata
     *
     * @param ctx DML table expression context
     */
    void init(PlSqlParser.Dml_table_expression_clauseContext ctx)
    {
        String tableName = getTableName(ctx.tableview_name());
        table = parser.databaseTables().forTable(catalogName, schemaName, tableName);
        if (table == null) {
            throw new ParsingException(null, "Trying to parse a table, which does not exist.");
        }
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            int type = column.jdbcType();
            T key = getKey(column, i);
            String name = ParserUtils.stripeQuotes(column.name().toUpperCase());
            newColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
            oldColumnValues.put(key, new LogMinerColumnValueWrapper(new LogMinerColumnValueImpl(name, type)));
        }
    }
}
