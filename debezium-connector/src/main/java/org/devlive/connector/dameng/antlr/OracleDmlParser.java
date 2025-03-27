/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.antlr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import org.devlive.connector.dameng.DamengValueConverters;
import org.devlive.connector.dameng.antlr.listener.OracleDmlParserListener;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This is the main Oracle Antlr DML parser
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public class OracleDmlParser
        extends AntlrDdlParser<PlSqlLexer, PlSqlParser>
{
    protected final String catalogName;
    protected final String schemaName;
    private final DamengValueConverters converter;
    private LogMinerDmlEntry dmlEntry;

    public OracleDmlParser(boolean throwErrorsFromTreeWalk, final String catalogName, final String schemaName, DamengValueConverters converter)
    {
        super(throwErrorsFromTreeWalk);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = converter;
    }

    public LogMinerDmlEntry getDmlEntry()
    {
        return dmlEntry;
    }

    public void setDmlEntry(LogMinerDmlEntry dml)
    {
        this.dmlEntry = dml;
    }

    @Override
    public void parse(String dmlContent, Tables databaseTables)
    {
        if (!dmlContent.endsWith(";")) {
            dmlContent = dmlContent + ";";
        }
        // DML content is case sensitive
        super.parse(dmlContent, databaseTables);
    }

    @Override
    public ParseTree parseTree(PlSqlParser parser)
    {
        return parser.unit_statement();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener()
    {
        return new OracleDmlParserListener(catalogName, schemaName, this);
    }

    @Override
    protected PlSqlLexer createNewLexerInstance(CharStream charStreams)
    {
        return new PlSqlLexer(charStreams);
    }

    @Override
    protected PlSqlParser createNewParserInstance(CommonTokenStream commonTokenStream)
    {
        return new PlSqlParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase()
    {
        return true;
    }

    @Override
    protected DataTypeResolver initializeDataTypeResolver()
    {
        return null;
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance()
    {
        return null;
    }

    public DamengValueConverters getConverters()
    {
        return converter;
    }
}
