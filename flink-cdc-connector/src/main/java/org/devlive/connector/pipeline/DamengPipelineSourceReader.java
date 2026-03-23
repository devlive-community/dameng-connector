package org.devlive.connector.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.core.io.InputStatus;
import org.devlive.connector.dameng.DamengConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Flink CDC Pipeline 的达梦数据库 SourceReader。
 * 内部运行 Debezium 引擎，将 CDC 记录转换为 Pipeline Event 并发射。
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class DamengPipelineSourceReader
        implements org.apache.flink.api.connector.source.SourceReader<Event, DamengPipelineSplit>
{
    private static final Logger LOG = LoggerFactory.getLogger(DamengPipelineSourceReader.class);

    private static final int QUEUE_CAPACITY = 10000;

    private final Properties debeziumProperties;
    private final BlockingQueue<Event> eventQueue;
    private final ObjectMapper objectMapper;
    private final Map<String, Schema> tableSchemas;

    private ExecutorService executor;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private volatile boolean running;
    private volatile boolean splitReceived;
    private CompletableFuture<Void> availability;

    public DamengPipelineSourceReader(Properties debeziumProperties, SourceReaderContext context)
    {
        this.debeziumProperties = debeziumProperties;
        this.eventQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.objectMapper = new ObjectMapper();
        this.tableSchemas = new HashMap<>();
        this.availability = new CompletableFuture<>();
    }

    @Override
    public void start()
    {
        // Debezium 引擎将在 addSplits() 收到分片后启动
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Event> output) throws Exception
    {
        if (!running) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        Event event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
        if (event != null) {
            output.collect(event);
            if (!eventQueue.isEmpty()) {
                return InputStatus.MORE_AVAILABLE;
            }
            return InputStatus.MORE_AVAILABLE;
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<DamengPipelineSplit> snapshotState(long checkpointId)
    {
        if (splitReceived) {
            return Collections.singletonList(new DamengPipelineSplit());
        }
        return Collections.emptyList();
    }

    @Override
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP"})
    public CompletableFuture<Void> isAvailable()
    {
        return availability;
    }

    @Override
    public void addSplits(List<DamengPipelineSplit> splits)
    {
        if (!splits.isEmpty() && !splitReceived) {
            splitReceived = true;
            startDebeziumEngine();
        }
    }

    @Override
    public void notifyNoMoreSplits()
    {
        // 对于无界 CDC 源，不会触发此方法
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        if (engine != null) {
            try {
                engine.close();
            }
            catch (Exception e) {
                LOG.warn("Error closing Debezium engine", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void startDebeziumEngine()
    {
        Properties props = new Properties();
        props.putAll(debeziumProperties);
        props.setProperty("connector.class", DamengConnector.class.getName());

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    try {
                        handleRecord(record);
                    }
                    catch (Exception e) {
                        LOG.error("Error processing Debezium record", e);
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "dameng-pipeline-debezium");
            t.setDaemon(true);
            return t;
        });

        running = true;
        executor.submit(engine);
        availability.complete(null);
    }

    private void offerEvent(Event event)
    {
        if (!eventQueue.offer(event)) {
            LOG.warn("Event queue full, dropping event: {}", event.getClass().getSimpleName());
        }
    }

    private void handleRecord(ChangeEvent<String, String> record) throws Exception
    {
        String value = record.value();
        if (value == null) {
            return;
        }

        JsonNode valueNode = objectMapper.readTree(value);
        JsonNode payload = valueNode.path("payload");

        if (payload.isMissingNode()) {
            return;
        }

        JsonNode source = payload.path("source");
        String schemaName = source.path("schema").asText(source.path("db").asText("PUBLIC"));
        String tableName = source.path("table").asText();

        if (tableName.isEmpty()) {
            return;
        }

        TableId tableId = TableId.tableId(schemaName, tableName);
        String tableKey = schemaName + "." + tableName;

        // 第一次遇到此表时，提取并发射 CreateTableEvent
        if (!tableSchemas.containsKey(tableKey)) {
            JsonNode keySchemaNode = valueNode.path("schema");
            String keyStr = record.key();
            JsonNode keySchema = null;
            if (keyStr != null) {
                keySchema = objectMapper.readTree(keyStr).path("schema");
            }

            List<String> primaryKeys = DamengSchemaConverter.extractPrimaryKeys(keySchema);
            JsonNode rowFields = DamengSchemaConverter.extractRowFields(
                    keySchemaNode.isMissingNode() ? null : keySchemaNode);

            Schema schema = DamengSchemaConverter.fromDebeziumSchema(rowFields, primaryKeys);
            tableSchemas.put(tableKey, schema);
            if (!eventQueue.offer(new CreateTableEvent(tableId, schema))) {
                    LOG.warn("Event queue full, dropping CreateTableEvent for {}", tableId);
                }
        }

        Schema schema = tableSchemas.get(tableKey);
        String op = payload.path("op").asText("r");

        JsonNode beforeNode = payload.path("before");
        JsonNode afterNode = payload.path("after");

        switch (op) {
            case "c":
            case "r":
                if (!afterNode.isNull() && !afterNode.isMissingNode()) {
                    Map<String, Object> afterMap = objectMapper.convertValue(afterNode, Map.class);
                    offerEvent(DataChangeEvent.insertEvent(tableId, new DamengRecordData(afterMap, schema)));
                }
                break;
            case "u":
                if (!beforeNode.isNull() && !beforeNode.isMissingNode()
                        && !afterNode.isNull() && !afterNode.isMissingNode()) {
                    Map<String, Object> beforeMap = objectMapper.convertValue(beforeNode, Map.class);
                    Map<String, Object> afterMap = objectMapper.convertValue(afterNode, Map.class);
                    offerEvent(DataChangeEvent.updateEvent(
                            tableId,
                            new DamengRecordData(beforeMap, schema),
                            new DamengRecordData(afterMap, schema)));
                }
                break;
            case "d":
                if (!beforeNode.isNull() && !beforeNode.isMissingNode()) {
                    Map<String, Object> beforeMap = objectMapper.convertValue(beforeNode, Map.class);
                    offerEvent(DataChangeEvent.deleteEvent(tableId, new DamengRecordData(beforeMap, schema)));
                }
                break;
            default:
                LOG.debug("Skipping unsupported operation: {}", op);
                break;
        }
    }
}
