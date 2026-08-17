/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import org.devlive.connector.dameng.antlr.OracleDdlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle.
 *
 * @author Gunnar Morling
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class BaseOracleSchemaChangeEventEmitter
        implements SchemaChangeEventEmitter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseOracleSchemaChangeEventEmitter.class);

    private final DamengOffsetContext offsetContext;
    private final TableId tableId;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final String commandType;
    private final DamengDatabaseSchema schema;

    public BaseOracleSchemaChangeEventEmitter(DamengOffsetContext offsetContext, TableId tableId,
            String sourceDatabaseName, String objectOwner, String ddlText,
            String commandType, DamengDatabaseSchema schema)
    {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.commandType = commandType;
        this.schema = schema;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver)
            throws InterruptedException
    {
        SchemaChangeEventType eventType = getSchemaChangeEventType();
        if (eventType == null) {
            return;
        }

        // Cache the table definition prior to parsing; it is needed to describe a DROP (the parser
        // removes the table) and lets ALTER be applied against the already-known table.
        final Table tableBefore = schema != null ? schema.tableFor(tableId) : null;

        // Seed a working copy with the existing table so ALTER/DROP can resolve it, then drain the
        // seeding change so drainChanges() below reflects only the parsed DDL.
        final Tables tables = new Tables();
        if (tableBefore != null) {
            tables.overwriteTable(tableBefore);
            tables.drainChanges();
        }

        final OracleDdlParser parser = new OracleDdlParser();
        parser.setCurrentDatabase(sourceDatabaseName);
        parser.setCurrentSchema(objectOwner);
        parser.parse(ddlText, tables);

        final Set<TableId> changedTableIds = tables.drainChanges();
        if (changedTableIds.isEmpty()) {
            throw new IllegalArgumentException("Couldn't parse DDL statement " + ddlText);
        }

        final SchemaChangeEvent event;
        switch (eventType) {
            case CREATE:
                event = SchemaChangeEvent.ofCreate(
                        offsetContext.asPartition(),
                        offsetContext,
                        sourceDatabaseName,
                        objectOwner,
                        ddlText,
                        tables.forTable(tableId),
                        false);
                break;
            case ALTER:
                event = SchemaChangeEvent.ofAlter(
                        offsetContext.asPartition(),
                        offsetContext,
                        sourceDatabaseName,
                        objectOwner,
                        ddlText,
                        tables.forTable(tableId));
                break;
            case DROP:
                if (tableBefore == null) {
                    LOGGER.warn("Ignoring DROP for unknown table {}: {}", tableId, ddlText);
                    return;
                }
                event = SchemaChangeEvent.ofDrop(
                        offsetContext.asPartition(),
                        offsetContext,
                        sourceDatabaseName,
                        objectOwner,
                        ddlText,
                        tableBefore);
                break;
            default:
                LOGGER.debug("Ignoring DDL event of type {}: {}", eventType, ddlText);
                return;
        }

        receiver.schemaChangeEvent(event);
    }

    private SchemaChangeEventType getSchemaChangeEventType()
    {
        switch (commandType) {
            case "CREATE TABLE":
                return SchemaChangeEventType.CREATE;
            case "ALTER TABLE":
                return SchemaChangeEventType.ALTER;
            case "DROP TABLE":
                return SchemaChangeEventType.DROP;
            default:
                LOGGER.debug("Ignoring DDL event of type {}", commandType);
        }

        return null;
    }
}
