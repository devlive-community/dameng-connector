/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.DamengConnection;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengConnectorConfig.LogMiningDmlParser;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.DamengValueConverters;
import org.devlive.connector.dameng.MapBackedPartition;
import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.logminer.parser.DmlParser;
import org.devlive.connector.dameng.logminer.parser.DmlParserException;
import org.devlive.connector.dameng.logminer.parser.LogMinerDmlParser;
import org.devlive.connector.dameng.logminer.parser.SimpleDmlParser;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * This class process entries obtained from LogMiner view.
 * It parses each entry.
 * On each DML it registers a callback in TransactionalBuffer.
 * On rollback it removes registered entries from TransactionalBuffer.
 * On commit it executes all registered callbacks, which dispatch ChangeRecords.
 * This also calculates metrics
 */
@SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT")
class LogMinerQueryResultProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerQueryResultProcessor.class);

    private final ChangeEventSourceContext context;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;
    private final TransactionalBuffer transactionalBuffer;
    private final DmlParser dmlParser;
    private final DamengOffsetContext offsetContext;
    private final DamengDatabaseSchema schema;
    private final EventDispatcher<MapBackedPartition, TableId> dispatcher;
    private final DamengConnectorConfig connectorConfig;
    private final Clock clock;
    private final HistoryRecorder historyRecorder;

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private long stuckScnCounter = 0;

    LogMinerQueryResultProcessor(
            ChangeEventSourceContext context,
            DamengConnection jdbcConnection,
            DamengConnectorConfig connectorConfig,
            DamengStreamingChangeEventSourceMetrics streamingMetrics,
            TransactionalBuffer transactionalBuffer,
            DamengOffsetContext offsetContext,
            DamengDatabaseSchema schema,
            EventDispatcher<MapBackedPartition, TableId> dispatcher,
            Clock clock,
            HistoryRecorder historyRecorder
    )
    {
        this.context = context;
        this.streamingMetrics = streamingMetrics;
        this.transactionalBuffer = transactionalBuffer;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.historyRecorder = historyRecorder;
        this.connectorConfig = connectorConfig;
        this.dmlParser = resolveParser(connectorConfig, jdbcConnection);
    }

    private static DmlParser resolveParser(DamengConnectorConfig connectorConfig, DamengConnection connection)
    {
        if (connectorConfig.getLogMiningDmlParser().equals(LogMiningDmlParser.LEGACY)) {
            DamengValueConverters converter = new DamengValueConverters(connectorConfig, connection);
            return new SimpleDmlParser(connectorConfig.getCatalogName(), converter);
        }
        return new LogMinerDmlParser();
    }

    /**
     * This method does all the job
     *
     * @param resultSet the info from LogMiner view
     * @return number of processed DMLs from the given resultSet
     * @throws SQLException thrown if any database exception occurs
     */
    void processResult(ResultSet resultSet)
            throws SQLException
    {
        int dmlCounter = 0;
        int insertCounter = 0;
        int updateCounter = 0;
        int deleteCounter = 0;
        int commitCounter = 0;
        int rollbackCounter = 0;
        long rows = 0;
        Instant startTime = Instant.now();
        while (context.isRunning() && hasNext(resultSet)) {
            rows++;
            Scn scn = Scn.valueOf(resultSet.getString("SCN"));
            String tableName = resultSet.getString("TABLE_NAME");
            String segOwner = resultSet.getString("SEG_OWNER");
            int operationCode = resultSet.getInt("OPERATION_CODE");
            Timestamp changeTime = resultSet.getTimestamp("TIMESTAMP", RowMapper.UTC_CALENDAR1);
            // String txId = RowMapper.getTransactionId(resultSet);
            String txId = resultSet.getString("XID");
            String operation = resultSet.getString("OPERATION");
            String userName = resultSet.getString("USERNAME");
            String rowId = resultSet.getString("ROW_ID");
            int rollbackFlag = resultSet.getInt("ROLL_BACK");

            boolean isDml = operationCode == 1 || operationCode == 3 || operationCode == 2;
            String redoSql = RowMapper.getSqlRedo(resultSet, isDml, historyRecorder, scn, tableName, segOwner, operationCode, changeTime, txId);

            LOGGER.trace("scn={}, operationCode={}, operation={}, table={}, segOwner={}, userName={}, rowId={}, rollbackFlag={}", scn, operationCode, operation,
                    tableName, segOwner, userName, rowId, rollbackFlag);

            String logMessage = String.format("transactionId=%s, SCN=%s, table_name=%s, segOwner=%s, operationCode=%s, offsetSCN=%s, " +
                    " commitOffsetSCN=%s", txId, scn, tableName, segOwner, operationCode, offsetContext.getScn(), offsetContext.getCommitScn());

            if (scn.isNull()) {
                LogMinerHelper.logWarn(streamingMetrics, "Scn is null for {}", logMessage);
                return;
            }

            // Commit
            if (operationCode == RowMapper.COMMIT) {
                if (transactionalBuffer.isTransactionRegistered(txId)) {
                    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                }
                if (transactionalBuffer.commit(txId, scn, offsetContext, changeTime, context, logMessage, dispatcher)) {
                    LOGGER.debug("COMMIT, {}", logMessage);
                    commitCounter++;
                }
                continue;
            }

            // Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                if (transactionalBuffer.isTransactionRegistered(txId)) {
                    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                }
                if (transactionalBuffer.rollback(txId, logMessage)) {
                    LOGGER.trace("ROLLBACK, {}", logMessage);
                    rollbackCounter++;
                }
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                // todo: DDL operations are not yet supported during streaming while using LogMiner.
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                LOGGER.info("DDL: {}, REDO_SQL: {}", logMessage, redoSql);
                continue;
            }

            // MISSING_SCN
            if (operationCode == RowMapper.MISSING_SCN) {
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                LogMinerHelper.logWarn(streamingMetrics, "Missing SCN,  {}", logMessage);
                continue;
            }

            // DML
            if (isDml) {
                final TableId tableId = RowMapper.getTableId(connectorConfig.getCatalogName(), resultSet);

                LOGGER.trace("DML,  {}, sql {}", logMessage, redoSql);
                if (redoSql == null) {
                    LOGGER.trace("Redo SQL was empty, DML operation skipped.");
                    continue;
                }

                dmlCounter++;
                switch (operationCode) {
                    case RowMapper.INSERT:
                        insertCounter++;
                        break;
                    case RowMapper.UPDATE:
                        updateCounter++;
                        break;
                    case RowMapper.DELETE:
                        deleteCounter++;
                        break;
                }

                final Table table = schema.tableFor(tableId);
                if (table == null) {
                    LogMinerHelper.logWarn(streamingMetrics, "DML for table '{}' that is not known to this connector, skipping.", tableId);
                    continue;
                }

                if (rollbackFlag == 1) {
                    // DML operation is to undo partial or all operations as a result of a rollback.
                    // This can be situations where an insert or update causes a constraint violation
                    // and a subsequent operation is written to the logs to revert the change.
                    transactionalBuffer.undoDmlOperation(txId, rowId, tableId);
                    continue;
                }

                final LogMinerDmlEntry dmlEntry = parse(redoSql, table, txId);
                dmlEntry.setObjectOwner(segOwner);
                dmlEntry.setSourceTime(changeTime);
                dmlEntry.setTransactionId(txId);
                dmlEntry.setObjectName(tableName);
                dmlEntry.setScn(scn);
                dmlEntry.setRowId(rowId);

                transactionalBuffer.registerDmlOperation(operationCode, txId, scn, tableId, dmlEntry, changeTime.toInstant(), rowId);
            }
        }

        Duration totalTime = Duration.between(startTime, Instant.now());
        if (dmlCounter > 0 || commitCounter > 0 || rollbackCounter > 0) {
            streamingMetrics.setLastCapturedDmlCount(dmlCounter);
            streamingMetrics.setLastDurationOfBatchProcessing(totalTime);

            warnStuckScn();
            currentOffsetScn = offsetContext.getScn();
            if (offsetContext.getCommitScn() != null) {
                currentOffsetCommitScn = offsetContext.getCommitScn();
            }
        }

        LOGGER.debug("{} Rows, {} DMLs, {} Commits, {} Rollbacks, {} Inserts, {} Updates, {} Deletes. Processed in {} millis. " +
                        "Lag:{}. Offset scn:{}. Offset commit scn:{}. Active transactions:{}. Sleep time:{}",
                rows, dmlCounter, commitCounter, rollbackCounter, insertCounter, updateCounter, deleteCounter, totalTime.toMillis(),
                streamingMetrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(), offsetContext.getCommitScn(),
                streamingMetrics.getNumberOfActiveTransactions(), streamingMetrics.getMillisecondToSleepBetweenMiningQuery());

        streamingMetrics.addProcessedRows(rows);
        historyRecorder.flush();

        transactionalBuffer.checkAndAutoCommitTransactions(offsetContext, context, dispatcher);
    }

    private boolean hasNext(ResultSet resultSet)
            throws SQLException
    {
        Instant rsNextStart = Instant.now();
        if (resultSet.next()) {
            streamingMetrics.addCurrentResultSetNext(Duration.between(rsNextStart, Instant.now()));
            return true;
        }
        return false;
    }

    /**
     * This method is warning if a long running transaction is discovered and could be abandoned in the future.
     * The criteria is the offset SCN remains the same in 25 mining cycles
     */
    private void warnStuckScn()
    {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            final Scn scn = offsetContext.getScn();
            final Scn commitScn = offsetContext.getCommitScn();
            if (currentOffsetScn.equals(scn) && !currentOffsetCommitScn.equals(commitScn)) {
                stuckScnCounter++;
                // logWarn only once
                if (stuckScnCounter == 25) {
                    LogMinerHelper.logWarn(streamingMetrics,
                            "Offset SCN {} is not changing. It indicates long transaction(s). " +
                                    "Offset commit SCN: {}",
                            currentOffsetScn, commitScn);
                    streamingMetrics.incrementScnFreezeCount();
                }
            }
            else {
                stuckScnCounter = 0;
            }
        }
    }

    private LogMinerDmlEntry parse(String redoSql, Table table, String txId)
    {
        LogMinerDmlEntry dmlEntry;
        try {
            Instant parseStart = Instant.now();
            dmlEntry = dmlParser.parse(redoSql, table, txId);
            streamingMetrics.addCurrentParseTime(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            StringBuilder message = new StringBuilder();
            message.append("DML statement couldn't be parsed.");
            message.append(" Please open a Jira issue with the statement '").append(redoSql).append("'.");
            if (LogMiningDmlParser.FAST.equals(connectorConfig.getLogMiningDmlParser())) {
                message.append(" You can set internal.log.mining.dml.parser='legacy' as a workaround until the parse error is fixed.");
            }
            throw new DmlParserException(message.toString(), e);
        }

        if (dmlEntry.getOldValues().isEmpty()) {
            if (Operation.UPDATE.equals(dmlEntry.getCommandType()) || Operation.DELETE.equals(dmlEntry.getCommandType())) {
                LOGGER.warn("The DML event '{}' contained no before state.", redoSql);
                streamingMetrics.incrementWarningCount();
            }
        }

        return dmlEntry;
    }
}
