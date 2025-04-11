/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.antlr.listener;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.TableId;
import org.devlive.connector.dameng.antlr.OracleDdlParser;

import static org.devlive.connector.dameng.antlr.listener.ParserUtils.getTableName;

/**
 * This class is parsing Oracle drop table statements.
 */
public class DropTableParserListener
        extends PlSqlParserBaseListener
{
    private final String catalogName;
    private final String schemaName;
    private final OracleDdlParser parser;

    DropTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterDrop_table(final PlSqlParser.Drop_tableContext ctx)
    {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name().get(0)));
        parser.databaseTables().removeTable(tableId);
        super.enterDrop_table(ctx);
    }
}
