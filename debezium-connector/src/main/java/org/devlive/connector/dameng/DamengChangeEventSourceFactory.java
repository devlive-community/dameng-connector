/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.logminer.LogMinerStreamingChangeEventSource;

import java.util.Optional;

@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "DLS_DEAD_LOCAL_STORE"})
public class DamengChangeEventSourceFactory
        implements ChangeEventSourceFactory<MapBackedPartition, DamengOffsetContext>
{
    private final DamengConnectorConfig configuration;
    private final DamengConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MapBackedPartition, TableId> dispatcher;
    private final Clock clock;
    private final DamengDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final DamengTaskContext taskContext;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    public DamengChangeEventSourceFactory(
            DamengConnectorConfig configuration,
            DamengConnection jdbcConnection,
            ErrorHandler errorHandler,
            EventDispatcher<MapBackedPartition, TableId> dispatcher,
            Clock clock,
            DamengDatabaseSchema schema,
            Configuration jdbcConfig,
            DamengTaskContext taskContext,
            DamengStreamingChangeEventSourceMetrics streamingMetrics
    )
    {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<MapBackedPartition, DamengOffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<MapBackedPartition> snapshotProgressListener)
    {
        return new DamengSnapshotChangeEventSource(
                configuration,
                jdbcConnection,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener
        );
    }

    @Override
    public StreamingChangeEventSource<MapBackedPartition, DamengOffsetContext> getStreamingChangeEventSource()
    {
        return new LogMinerStreamingChangeEventSource(
                configuration,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics
        );
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MapBackedPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            DamengOffsetContext offsetContext,
            SnapshotProgressListener<MapBackedPartition> snapshotProgressListener,
            DataChangeEventListener<MapBackedPartition> dataChangeEventListener)
    {
        // 如果需要实现增量快照，可以在这里返回实现
        // 默认返回空实现
        return Optional.empty();
    }
}
