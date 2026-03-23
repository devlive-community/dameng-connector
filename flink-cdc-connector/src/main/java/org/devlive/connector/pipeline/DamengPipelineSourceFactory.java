package org.devlive.connector.pipeline;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.source.DataSource;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 达梦数据库 Flink CDC Pipeline DataSourceFactory。
 * 在 YAML Pipeline 配置文件中通过 type: dameng 引用此工厂。
 *
 * <p>示例 YAML 配置：
 * <pre>
 * source:
 *   type: dameng
 *   hostname: localhost
 *   port: 5236
 *   username: SYSDBA
 *   password: SYSDBAPASS
 *   database: TEST
 *   schema-list: PUBLIC
 *   table-list: PUBLIC.USER_TABLE
 *   server-id: 5001
 *   server-name: dameng-server
 * </pre>
 */
public class DamengPipelineSourceFactory
        implements DataSourceFactory
{
    public static final String IDENTIFIER = "dameng";

    public static final ConfigOption<String> HOSTNAME = ConfigOptions
            .key("hostname")
            .stringType()
            .noDefaultValue()
            .withDescription("达梦数据库服务器主机名或 IP 地址。");

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port")
            .intType()
            .defaultValue(5236)
            .withDescription("达梦数据库服务器端口号，默认 5236。");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("连接达梦数据库的用户名。");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("连接达梦数据库的密码。");

    public static final ConfigOption<String> DATABASE = ConfigOptions
            .key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("要监控的达梦数据库名称。");

    public static final ConfigOption<String> SCHEMA_LIST = ConfigOptions
            .key("schema-list")
            .stringType()
            .noDefaultValue()
            .withDescription("要监控的 schema 列表，逗号分隔。");

    public static final ConfigOption<String> TABLE_LIST = ConfigOptions
            .key("table-list")
            .stringType()
            .noDefaultValue()
            .withDescription("要监控的表列表，格式为 schema.table，逗号分隔，支持正则。");

    public static final ConfigOption<String> SERVER_ID = ConfigOptions
            .key("server-id")
            .stringType()
            .defaultValue("5001")
            .withDescription("数据库服务器唯一标识符。");

    public static final ConfigOption<String> SERVER_NAME = ConfigOptions
            .key("server-name")
            .stringType()
            .defaultValue("dameng-server")
            .withDescription("连接器逻辑名称，用于标识此 CDC 数据源。");

    public static final ConfigOption<String> SNAPSHOT_MODE = ConfigOptions
            .key("snapshot-mode")
            .stringType()
            .defaultValue("initial")
            .withDescription("快照模式：initial（默认）或 schema_only。");

    public static final ConfigOption<String> HISTORY_FILE = ConfigOptions
            .key("history-file")
            .stringType()
            .defaultValue("dameng-pipeline-history.txt")
            .withDescription("数据库 schema 历史文件路径。");

    @Override
    public String identifier()
    {
        return IDENTIFIER;
    }

    @Override
    public DataSource createDataSource(Context context)
    {
        Map<String, String> options = context.getFactoryConfiguration().toMap();

        Properties properties = new Properties();

        // 连接配置
        String hostname = getRequired(options, HOSTNAME.key());
        properties.setProperty("database.hostname", hostname);

        String port = options.getOrDefault(PORT.key(), String.valueOf(PORT.defaultValue()));
        properties.setProperty("database.port", port);

        String username = getRequired(options, USERNAME.key());
        properties.setProperty("database.user", username);

        String password = getRequired(options, PASSWORD.key());
        properties.setProperty("database.password", password);

        String database = getRequired(options, DATABASE.key());
        properties.setProperty("database.dbname", database);

        // 服务器标识
        String serverId = options.getOrDefault(SERVER_ID.key(), SERVER_ID.defaultValue());
        properties.setProperty("database.server.id", serverId);

        String serverName = options.getOrDefault(SERVER_NAME.key(), SERVER_NAME.defaultValue());
        properties.setProperty("database.server.name", serverName);

        // 表过滤
        if (options.containsKey(TABLE_LIST.key())) {
            String tableList = options.get(TABLE_LIST.key());
            // 将逗号分隔的 schema.table 转换为 Debezium table.include.list 格式（正则）
            String debeziumTableList = tableList.replace(".", "\\.").replace(",", ",");
            properties.setProperty("table.include.list", debeziumTableList);
        }

        // 快照模式
        String snapshotMode = options.getOrDefault(SNAPSHOT_MODE.key(), SNAPSHOT_MODE.defaultValue());
        properties.setProperty("snapshot.mode", snapshotMode);

        // 历史文件
        String historyFile = options.getOrDefault(HISTORY_FILE.key(), HISTORY_FILE.defaultValue());
        properties.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", historyFile);

        // 默认适配器
        properties.setProperty("database.connection.adapter", "LogMiner");
        properties.setProperty("database.serverTimezone", "UTC");

        // 传递所有以 debezium. 开头的自定义属性
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith("debezium.")) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
        }

        return new DamengPipelineSource(properties);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions()
    {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions()
    {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SCHEMA_LIST);
        options.add(TABLE_LIST);
        options.add(SERVER_ID);
        options.add(SERVER_NAME);
        options.add(SNAPSHOT_MODE);
        options.add(HISTORY_FILE);
        return options;
    }

    private String getRequired(Map<String, String> options, String key)
    {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Required option '" + key + "' is missing in DamengPipelineSourceFactory configuration.");
        }
        return value;
    }
}
