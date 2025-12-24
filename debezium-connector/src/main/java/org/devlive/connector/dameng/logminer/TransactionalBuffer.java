/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.apache.kafka.connect.errors.DataException;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.MapBackedPartition;
import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Buffer that stores transactions and related callbacks that will be executed when a transaction commits or discarded
 * when a transaction has been rolled back.
 *
 * @author Andrey Pustovetov
 */
@NotThreadSafe
public final class TransactionalBuffer
        implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final Map<String, Transaction> transactions;
    private final DamengDatabaseSchema schema;
    private final Clock clock;
    private final ErrorHandler errorHandler;
    private final Set<String> abandonedTransactionIds;
    private final Set<String> rolledBackTransactionIds;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;
    private final long autoCommitTimeoutMs;

    private Scn lastCommittedScn;

    /**
     * Constructor to create a new instance.
     *
     * @param errorHandler the connector error handler
     * @param streamingMetrics the streaming metrics
     */
    TransactionalBuffer(DamengDatabaseSchema schema, Clock clock, ErrorHandler errorHandler, DamengStreamingChangeEventSourceMetrics streamingMetrics, long autoCommitTimeoutMs)
    {
        this.transactions = new LinkedHashMap<>();
        this.schema = schema;
        this.clock = clock;
        this.errorHandler = errorHandler;
        this.lastCommittedScn = Scn.NULL;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
        this.streamingMetrics = streamingMetrics;
        this.autoCommitTimeoutMs = autoCommitTimeoutMs;
    }

    /**
     * @return rolled back transactions
     */
    Set<String> getRolledBackTransactionIds()
    {
        return new HashSet<>(rolledBackTransactionIds);
    }

    /**
     * Register a DML operation with the transaction buffer.
     *
     * @param operation operation type
     * @param transactionId unique transaction identifier
     * @param scn system change number
     * @param tableId table identifier
     * @param parseEntry parser entry
     * @param changeTime time the DML operation occurred
     * @param rowId unique row identifier
     */
    void registerDmlOperation(int operation, String transactionId, Scn scn, TableId tableId, LogMinerDmlEntry parseEntry, Instant changeTime, String rowId)
    {
        if (abandonedTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Captured DML for abandoned transaction {}, ignored.", transactionId);
            return;
        }
        if (rolledBackTransactionIds.contains(transactionId)) {
            LogMinerHelper.logWarn(streamingMetrics, "Captured DML for rolled back transaction {}, ignored.", transactionId);
            return;
        }

        Transaction transaction = transactions.computeIfAbsent(transactionId, s -> new Transaction(transactionId, scn));
        transaction.events.add(new DmlEvent(operation, parseEntry, scn, tableId, rowId));

        // 来记录最后更新时间
        transaction.lastUpdateTime = Instant.now();

        streamingMetrics.setActiveTransactions(transactions.size());
        streamingMetrics.incrementRegisteredDmlCount();
        streamingMetrics.calculateLagMetrics(changeTime);
    }

    /**
     * Undo a staged DML operation in the transaction buffer.
     *
     * @param transactionId unique transaction identifier
     * @param undoRowId unique row identifier to be undone
     * @param tableId table identifier
     */
    void undoDmlOperation(String transactionId, String undoRowId, TableId tableId)
    {
        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            LOGGER.warn("Cannot undo changes to {} with row id {} as transaction {} not found.", tableId, undoRowId, transactionId);
            return;
        }

        transaction.events.removeIf(o -> {
            if (o.getRowId().equals(undoRowId)) {
                LOGGER.trace("Undoing change to {} with row id {} in transaction {}", tableId, undoRowId, transactionId);
                return true;
            }
            return false;
        });
    }

    /**
     * Commits a transaction by looking up the transaction in the buffer and if exists, all registered callbacks
     * will be executed in chronological order, emitting events for each followed by a transaction commit event.
     *
     * @param transactionId transaction identifier
     * @param scn SCN of the commit.
     * @param offsetContext Oracle offset
     * @param timestamp commit timestamp
     * @param context context to check that source is running
     * @param debugMessage message
     * @param dispatcher event dispatcher
     * @return true if committed transaction is in the buffer, was not processed yet and processed now
     */
    boolean commit(
            String transactionId,
            Scn scn,
            DamengOffsetContext offsetContext,
            Timestamp timestamp,
            ChangeEventSource.ChangeEventSourceContext context,
            String debugMessage,
            EventDispatcher<MapBackedPartition, TableId> dispatcher
    )
    {
        Instant start = Instant.now();
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return false;
        }

        Scn smallestScn = calculateSmallestScn();
        abandonedTransactionIds.remove(transactionId);

        // On the restarting connector, we start from SCN in the offset. There is possibility to commit a transaction(s) which were already committed.
        // Currently we cannot use ">=", because we may lose normal commit which may happen at the same time. TODO use audit table to prevent duplications
        if ((offsetContext.getCommitScn() != null && offsetContext.getCommitScn().compareTo(scn) > 0) || lastCommittedScn.compareTo(scn) > 0) {
            LogMinerHelper.logWarn(streamingMetrics,
                    "Transaction {} was already processed, ignore. Committed SCN in offset is {}, commit SCN of the transaction is {}, last committed SCN is {}",
                    transactionId, offsetContext.getCommitScn(), scn, lastCommittedScn);
            streamingMetrics.setActiveTransactions(transactions.size());
            return false;
        }

        LOGGER.trace("COMMIT, {}, smallest SCN: {}", debugMessage, smallestScn);
        commit(context, offsetContext, start, transaction, timestamp, smallestScn, scn, dispatcher);
        return true;
    }

    private void commit(
            ChangeEventSource.ChangeEventSourceContext context,
            DamengOffsetContext offsetContext,
            Instant start,
            Transaction transaction,
            Timestamp timestamp,
            Scn smallestScn,
            Scn scn,
            EventDispatcher<MapBackedPartition, TableId> dispatcher
    )
    {
        try {
            int counter = transaction.events.size();
            for (DmlEvent event : transaction.events) {
                if (!context.isRunning()) {
                    return;
                }

                // Update SCN in offset context only if processed SCN less than SCN among other transactions
                if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                    offsetContext.setScn(event.getScn());
                    streamingMetrics.setOldestScn(event.getScn());
                }
                offsetContext.setTransactionId(transaction.transactionId);
                offsetContext.setSourceTime(timestamp.toInstant());
                offsetContext.setTableId(event.getTableId());
                if (--counter == 0) {
                    offsetContext.setCommitScn(scn);
                }

                LOGGER.trace("Processing DML event {} with SCN {}", event.getEntry(), event.getScn());

                // 获取当前分区并传递给dispatcher
                MapBackedPartition partition = offsetContext.asPartition();

                dispatcher.dispatchDataChangeEvent(
                        partition,
                        event.getTableId(),
                        new LogMinerChangeRecordEmitter(
                                partition,
                                offsetContext,
                                event.getEntry(),
                                schema.tableFor(event.getTableId()),
                                clock
                        )
                );
            }

            lastCommittedScn = Scn.valueOf(scn.longValue());

            if (!transaction.events.isEmpty()) {
                MapBackedPartition partition = offsetContext.asPartition();
                dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
            }
        }
        catch (InterruptedException e) {
            LogMinerHelper.logError(streamingMetrics, "Thread interrupted during running", e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            streamingMetrics.incrementCommittedTransactions();
            streamingMetrics.setActiveTransactions(transactions.size());
            streamingMetrics.incrementCommittedDmlCount(transaction.events.size());
            streamingMetrics.setCommittedScn(scn);
            streamingMetrics.setOffsetScn(offsetContext.getScn());
            streamingMetrics.setLastCommitDuration(Duration.between(start, Instant.now()));
        }
    }

    /**
     * 检查长时间未提交的事务，自动提交它们
     * <p>
     * 在处理完LogMiner查询结果后，检查所有已注册但尚未提交的事务。
     * 如果某个事务超过一定时间没有更新（默认为1秒），系统会自动提交它。
     * 这样，即使LogMiner没有捕获到COMMIT事件，操作也能被正确处理和提交。
     */
    void checkAndAutoCommitTransactions(
            DamengOffsetContext offsetContext,
            ChangeEventSource.ChangeEventSourceContext context,
            EventDispatcher<MapBackedPartition, TableId> dispatcher
    )
    {
        Instant now = Instant.now();
        Set<String> transactionsToCommit = new LinkedHashSet<>();

        // 找出所有超过指定时间未更新的事务
        for (Map.Entry<String, Transaction> entry : transactions.entrySet()) {
            if (Duration.between(entry.getValue().lastUpdateTime, now).toMillis() > autoCommitTimeoutMs) {
                transactionsToCommit.add(entry.getKey());
            }
        }

        // 提交这些事务
        for (String txId : transactionsToCommit) {
            Transaction tx = transactions.get(txId);
            if (tx != null && !tx.events.isEmpty()) {
                LOGGER.info("Auto-committing transaction {} with {} events due to timeout",
                        txId, tx.events.size());
                commit(txId, tx.lastScn, offsetContext, Timestamp.from(now), context,
                        "Auto-commit due to timeout", dispatcher);
            }
        }
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction id
     * @param debugMessage message
     * @return true if the rollback is for a transaction in the buffer
     */
    boolean rollback(String transactionId, String debugMessage)
    {
        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            LOGGER.debug("Transaction rolled back: {}", debugMessage);

            transactions.remove(transactionId);
            abandonedTransactionIds.remove(transactionId);
            rolledBackTransactionIds.add(transactionId);

            streamingMetrics.setActiveTransactions(transactions.size());
            streamingMetrics.incrementRolledBackTransactions();
            streamingMetrics.addRolledBackTransactionId(transactionId);
            return true;
        }
        return false;
    }

    /**
     * If for some reason the connector got restarted, the offset will point to the beginning of the oldest captured transaction.
     * If that transaction was lasted for a long time, let say > 4 hours, the offset might be not accessible after restart,
     * Hence we have to address these cases manually.
     * <p>
     * In case of an abandonment, all DMLs/Commits/Rollbacks for this transaction will be ignored
     *
     * @param thresholdScn the smallest SVN of any transaction to keep in the buffer. All others will be removed.
     * @param offsetContext the offset context
     */
    void abandonLongTransactions(Scn thresholdScn, DamengOffsetContext offsetContext)
    {
        LogMinerHelper.logWarn(streamingMetrics, "All transactions with first SCN <= {} will be abandoned, offset: {}", thresholdScn, offsetContext.getScn());
        Scn threshold = Scn.valueOf(thresholdScn.toString());
        Scn smallestScn = calculateSmallestScn();
        if (smallestScn == null) {
            // no transactions in the buffer
            return;
        }
        if (threshold.compareTo(smallestScn) < 0) {
            threshold = smallestScn;
        }
        Iterator<Map.Entry<String, Transaction>> iter = transactions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Transaction> transaction = iter.next();
            if (transaction.getValue().firstScn.compareTo(threshold) <= 0) {
                LogMinerHelper.logWarn(streamingMetrics, "Following long running transaction {} will be abandoned and ignored: {} ", transaction.getKey(),
                        transaction.getValue().toString());
                abandonedTransactionIds.add(transaction.getKey());
                iter.remove();

                streamingMetrics.addAbandonedTransactionId(transaction.getKey());
                streamingMetrics.setActiveTransactions(transactions.size());
            }
        }
    }

    boolean isTransactionRegistered(String txId)
    {
        return transactions.get(txId) != null;
    }

    private Scn calculateSmallestScn()
    {
        Scn scn = transactions.isEmpty() ? null
                : transactions.values()
                .stream()
                .map(transaction -> transaction.firstScn)
                .min(Scn::compareTo)
                .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
        streamingMetrics.setOldestScn(scn == null ? Scn.valueOf(-1) : scn);
        return scn;
    }

    /**
     * Returns {@code true} if buffer is empty, otherwise {@code false}.
     *
     * @return {@code true} if buffer is empty, otherwise {@code false}
     */
    boolean isEmpty()
    {
        return transactions.isEmpty();
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        this.transactions.values().forEach(t -> result.append(t.toString()));
        return result.toString();
    }

    @Override
    public void close()
    {
        transactions.clear();

        if (this.streamingMetrics != null) {
            this.streamingMetrics.unregister();
        }
    }

    /**
     * Represents a logical database transaction
     */
    private static final class Transaction
    {
        private final String transactionId;
        private final Scn firstScn;
        private final List<DmlEvent> events;
        private final Scn lastScn;
        private Instant lastUpdateTime;

        private Transaction(String transactionId, Scn firstScn)
        {
            this.transactionId = transactionId;
            this.firstScn = firstScn;
            this.events = new ArrayList<>();
            this.lastScn = firstScn;
            this.lastUpdateTime = Instant.now();
        }

        @Override
        public String toString()
        {
            return "Transaction{" +
                    "transactionId=" + transactionId +
                    ", firstScn=" + firstScn +
                    ", lastScn=" + lastScn +
                    ", lastUpdateTime=" + lastUpdateTime +
                    '}';
        }
    }

    /**
     * Represents a DML event for a given table row.
     */
    private static class DmlEvent
    {
        private final int operation;
        private final LogMinerDmlEntry entry;
        private final Scn scn;
        private final TableId tableId;
        private final String rowId;

        public DmlEvent(int operation, LogMinerDmlEntry entry, Scn scn, TableId tableId, String rowId)
        {
            this.operation = operation;
            this.scn = scn;
            this.tableId = tableId;
            this.rowId = rowId;
            this.entry = entry;
        }

        public int getOperation()
        {
            return operation;
        }

        public LogMinerDmlEntry getEntry()
        {
            return entry;
        }

        public Scn getScn()
        {
            return scn;
        }

        public TableId getTableId()
        {
            return tableId;
        }

        public String getRowId()
        {
            return rowId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DmlEvent dmlEvent = (DmlEvent) o;
            return operation == dmlEvent.operation &&
                    Objects.equals(entry, dmlEvent.entry) &&
                    Objects.equals(scn, dmlEvent.scn) &&
                    Objects.equals(tableId, dmlEvent.tableId) &&
                    Objects.equals(rowId, dmlEvent.rowId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operation, entry, scn, tableId, rowId);
        }
    }
}
