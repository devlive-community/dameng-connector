/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.devlive.connector.dameng.logminer.LogMinerHelper;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link StreamingChangeEventSource} for Oracle.
 *
 * @author Gunnar Morling
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
public class DamengSnapshotChangeEventSource<P extends Partition>
        extends RelationalSnapshotChangeEventSource<P, DamengOffsetContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengSnapshotChangeEventSource.class);

    private final DamengConnectorConfig connectorConfig;
    private final DamengConnection jdbcConnection;

    public DamengSnapshotChangeEventSource(
            DamengConnectorConfig connectorConfig,
            DamengConnection jdbcConnection,
            DamengDatabaseSchema schema,
            EventDispatcher<P, TableId> dispatcher,
            Clock clock,
            SnapshotProgressListener<P> snapshotProgressListener
    )
    {
        super(connectorConfig, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);

        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
    }

    private static String quote(TableId tableId)
    {
        return TableId.parse(tableId.schema() + "." + tableId.table(), true).toDoubleQuotedString();
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(P partition, DamengOffsetContext previousOffset)
    {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            snapshotData = connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<P, DamengOffsetContext> prepare(P partition)
            throws Exception
    {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        return new OracleSnapshotContext(partition, connectorConfig.getCatalogName());
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<P, DamengOffsetContext> ctx)
            throws Exception
    {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext)
            throws SQLException, InterruptedException
    {
        ((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

        try (Statement statement = jdbcConnection.connection().createStatement()) {
            for (TableId tableId : snapshotContext.capturedTables) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while locking table " + tableId);
                }

                LOGGER.debug("Locking table {}", tableId);
                statement.execute("LOCK TABLE " + quote(tableId) + " IN EXCLUSIVE MODE");
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext)
            throws SQLException
    {
        jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<P, DamengOffsetContext> ctx, DamengOffsetContext previousOffset)
            throws Exception
    {
        Optional<Scn> latestTableDdlScn = getLatestTableDdlScn(ctx);
        Scn currentScn;

        // we must use an SCN for taking the snapshot that represents a later timestamp than the latest DDL change than
        // any of the captured tables; this will not be a problem in practice, but during testing it may happen that the
        // SCN of "now" represents the same timestamp as a newly created table that should be captured; in that case
        // we'd get a ORA-01466 when running the flashback query for doing the snapshot
        do {
            currentScn = getCurrentScn(ctx);
        }
        while (areSameTimestamp(latestTableDdlScn.orElse(null), currentScn));

        ctx.offset = DamengOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .transactionContext(new TransactionContext())
                .build();
    }

    private Scn getCurrentScn(SnapshotContext<P, DamengOffsetContext> ctx)
            throws SQLException
    {
        if (connectorConfig.getAdapter().equals(DamengConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            return LogMinerHelper.getCurrentScn(jdbcConnection);
        }

        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT ARCH_LSN FROM SYS.V$ARCH_FILE WHERE  STATUS = 'ACTIVE'")) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            return Scn.valueOf(rs.getString(1));
        }
    }

    /**
     * Whether the two SCNs represent the same timestamp or not (resolution is only 3 seconds).
     */
    private boolean areSameTimestamp(Scn scn1, Scn scn2)
            throws SQLException
    {
        if (scn1 == null) {
            return false;
        }

        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(" + scn1 + ") = SCN_TO_TIMESTAMP(" + scn2 + ")")) {
            return rs.next();
        }
    }

    /**
     * Returns the SCN of the latest DDL change to the captured tables. The result will be empty if there's no table to
     * capture as per the configuration.
     */
    private Optional<Scn> getLatestTableDdlScn(RelationalSnapshotContext<P, DamengOffsetContext> ctx)
            throws SQLException
    {
        if (ctx.capturedTables.isEmpty()) {
            return Optional.empty();
        }

        StringBuilder lastDdlScnQuery = new StringBuilder("SELECT TIMESTAMP_TO_SCN(MAX(last_ddl_time))")
                .append(" FROM all_objects")
                .append(" WHERE");

        for (TableId table : ctx.capturedTables) {
            lastDdlScnQuery.append(" (owner = '" + table.schema() + "' AND object_name = '" + table.table() + "') OR");
        }

        String query = lastDdlScnQuery.substring(0, lastDdlScnQuery.length() - 3);
        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get latest table DDL SCN");
            }

            // Guard against LAST_DDL_TIME with value of 0.
            // This case should be treated as if we were unable to determine a value for LAST_DDL_TIME.
            // This forces later calculations to be based upon the current SCN.
            String latestDdlTime = rs.getString(1);
            if ("0".equals(latestDdlTime)) {
                return Optional.empty();
            }

            return Optional.of(Scn.valueOf(latestDdlTime));
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 8180) {
                // DBZ-1446 In this use case we actually do not want to propagate the exception but
                // rather return an empty optional value allowing the current SCN to take prior.
                LOGGER.info("No latest table SCN could be resolved, defaulting to current SCN");
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext, DamengOffsetContext offsetContext)
            throws SQLException, InterruptedException
    {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false
            );
        }
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext, String overriddenSelect, TableId tableId)
    {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext, Table table)
            throws SQLException
    {
        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT DBMS_METADATA.GET_DDL( 'TABLE', '" + table.id().table() + "', '" + table.id().schema() + "' ) FROM DUAL")) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get metadata");
            }

            Object res = rs.getObject(1);
            String ddl = ((Clob) res).getSubString(1, (int) ((Clob) res).length());

            // 在Debezium 1.9.8中，SchemaChangeEvent.ofCreate()方法需要一个非null的partition参数
            // 如果snapshotContext.partition为null，我们需要从偏移量上下文中获取一个有效的partition
            P partition = snapshotContext.partition;
            if (partition == null) {
                // 从偏移量上下文创建一个partition
                partition = (P) snapshotContext.offset.asPartition();

                // 如果仍然为null，记录警告并尝试创建一个空的MapBackedPartition
                if (partition == null) {
                    LOGGER.warn("Could not determine partition for table {}; using empty partition", table.id());
                    Map<String, String> partitionData = new HashMap<>();
                    partitionData.put("server", connectorConfig.getLogicalName());
                    partition = (P) new MapBackedPartition(partitionData);
                }
            }

            return SchemaChangeEvent.ofCreate(
                    partition,
                    snapshotContext.offset,
                    snapshotContext.catalogName,
                    table.id().schema(),
                    ddl,
                    table,
                    true
            );
        }
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<P, DamengOffsetContext> snapshotContext, TableId tableId, List<String> columns)
    {
        final DamengOffsetContext offset = snapshotContext.offset;
        final String snapshotOffset = offset.getScn().toString();
        if (snapshotOffset == null) {
            throw new IllegalStateException("Snapshot offset cannot be null");
        }
        return Optional.of("SELECT * FROM " + quote(tableId) + " AS OF SCN " + snapshotOffset);
    }

    @Override
    protected void complete(SnapshotContext<P, DamengOffsetContext> snapshotContext)
    {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.resetSessionToCdb();
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private class OracleSnapshotContext
            extends RelationalSnapshotContext<P, DamengOffsetContext>
    {
        private Savepoint preSchemaSnapshotSavepoint;

        public OracleSnapshotContext(P partition, String catalogName)
                throws SQLException
        {
            super(partition, catalogName);
        }
    }
}
