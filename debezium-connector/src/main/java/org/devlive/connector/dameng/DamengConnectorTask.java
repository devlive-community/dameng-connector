/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DamengConnectorTask
        extends BaseSourceTask<MapBackedPartition, DamengOffsetContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnectorTask.class);
    private static final String CONTEXT_NAME = "dameng-connector-task";

    private volatile DamengTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile DamengConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile DamengDatabaseSchema schema;

    @Override
    public String version()
    {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<MapBackedPartition, DamengOffsetContext> start(Configuration config)
    {
        DamengConnectorConfig connectorConfig = new DamengConnectorConfig(config);
        TopicSelector<TableId> topicSelector = DamengTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.jdbcConfig();
        jdbcConnection = new DamengConnection(jdbcConfig, () -> getClass().getClassLoader());
        this.schema = new DamengDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);
        this.schema.initializeStorage();

        String adapterString = config.getString(DamengConnectorConfig.CONNECTOR_ADAPTER);
        DamengConnectorConfig.ConnectorAdapter adapter = DamengConnectorConfig.ConnectorAdapter.parse(adapterString);

        // 使用新的API获取偏移量
        Offsets<MapBackedPartition, DamengOffsetContext> previousOffsets = getPreviousOffsets(
                new MapBackedPartitionProvider(connectorConfig),
                new DamengOffsetContext.Loader(connectorConfig, adapter));

        // 在Debezium 1.9.8中，recover方法期望Offsets对象而不是OffsetContext
        if (previousOffsets.getTheOnlyOffset() != null) {
            schema.recover(previousOffsets);
        }

        taskContext = new DamengTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new DamengErrorHandler(connectorConfig.getLogicalName(), queue);

        final DamengEventMetadataProvider metadataProvider = new DamengEventMetadataProvider();

        EventDispatcher<MapBackedPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final DamengStreamingChangeEventSourceMetrics streamingMetrics =
                new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider, connectorConfig);

        ChangeEventSourceFactory<MapBackedPartition, DamengOffsetContext> changeEventSourceFactory =
                new DamengChangeEventSourceFactory(
                        connectorConfig,
                        jdbcConnection,
                        errorHandler,
                        dispatcher,
                        clock,
                        schema,
                        jdbcConfig,
                        taskContext,
                        streamingMetrics);

        ChangeEventSourceCoordinator<MapBackedPartition, DamengOffsetContext> coordinator =
                new ChangeEventSourceCoordinator<>(
                        previousOffsets,
                        errorHandler,
                        DamengConnector.class,
                        connectorConfig,
                        changeEventSourceFactory,
                        new DamengChangeEventSourceMetricsFactory<>(streamingMetrics),
                        dispatcher,
                        schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll()
            throws InterruptedException
    {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop()
    {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields()
    {
        return DamengConnectorConfig.ALLFIELDS;
    }

    /**
     * 内部类实现分区提供者
     */
    private static class MapBackedPartitionProvider
            implements Partition.Provider<MapBackedPartition>
    {
        private final DamengConnectorConfig connectorConfig;

        public MapBackedPartitionProvider(DamengConnectorConfig connectorConfig)
        {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<MapBackedPartition> getPartitions()
        {
            Map<String, String> map = Maps.newHashMap();
            map.put("server", connectorConfig.getLogicalName());
            // 创建一个只包含逻辑名称的分区
            return Sets.newHashSet(new MapBackedPartition(map));
        }
    }
}
