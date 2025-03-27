package org.devlive.connector;

import org.devlive.connector.dameng.DamengConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DamengDebeziumConnectorTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengDebeziumConnectorTest.class);
    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.setProperty("name", "dameng-engine-localhost");
        props.setProperty("connector.class", DamengConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        props.setProperty("offset.storage.file.filename", "offset.txt");
        props.setProperty("offset.flush.interval.ms", "60000");
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5236");
        props.setProperty("database.user", "SYSDBA");
        props.setProperty("database.password", "SYSDBAPASS");
        props.setProperty("database.server.id", "85701");

        props.setProperty("table.include.list", "TEST\\..*");
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", "history.txt");
        String connectorName = "my-dameng-connector-" + getCurrentDateString();
        props.setProperty("database.server.name", connectorName);
        props.setProperty("database.dbname", "TEST");
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("database.serverTimezone", "UTC");
        props.setProperty("database.connection.adapter", "LogMiner");

        props.setProperty("debezium.log.mining.strategy", "online_catalog");
        props.setProperty("debezium.log.mining.continuous.mine", "true");
        props.setProperty("debezium.log.level", "DEBUG");

        props.setProperty("debezium.source.log.mining.batch.size", "1000");
        props.setProperty("debezium.source.poll.interval.ms", "1000");

        // 事务处理配置
//        props.setProperty("debezium.source.transaction.recover.policy", "skip"); // 或尝试 "skip"
//        props.setProperty("debezium.source.max.queue.size", "8192"); // 增加队列大小
//        props.setProperty("debezium.source.max.batch.size", "2048"); // 增加批处理大小

        // 暂时不能使用 "fast" 会导致部分字段解析失败
//        props.setProperty("internal.log.mining.dml.parser", "legacy");

        // 自动提交未提交事务的时间（以毫秒为单位）。
        props.setProperty("debezium.source.transaction.auto.commit.timeout.ms", "2000");

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> LOGGER.info("Record: {}", record))
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        LOGGER.error("Process status [ false ] with error message [ {} ], full stack trace: ", message, error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine)
    {
        try {
            engine.close();
        }
        catch (IOException ignored) {
        }
    }

    private static String getCurrentDateString()
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMdd");
        return dateFormat.format(new Date());
    }
}
