package org.devlive.connector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@SuppressFBWarnings(value = {"DLS_DEAD_LOCAL_STORE"})
public class DamengTableSourceFactory
        implements DynamicTableSourceFactory
{
    // 连接器标识符
    public static final String IDENTIFIER = "dameng-cdc";

    // 配置选项
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue()
            .withDescription("The hostname or IP address of the Dameng server.");

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .defaultValue(5236)
            .withDescription("Dameng server port number.");

    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("Dameng database username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Dameng database password.");

    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the Dameng database to monitor.");

    public static final ConfigOption<String> TABLE = ConfigOptions.key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the Dameng table to monitor.");

    public static final ConfigOption<String> SERVER_ID = ConfigOptions.key("server-id")
            .stringType()
            .noDefaultValue()
            .withDescription("The unique ID of the database server.");

    public static final ConfigOption<String> SERVER_NAME = ConfigOptions.key("server-name")
            .stringType()
            .noDefaultValue()
            .withDescription("A logical name that identifies and names the subject of this connector.");

    @Override
    public String factoryIdentifier()
    {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions()
    {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE);
        options.add(TABLE);
        options.add(SERVER_ID);
        options.add(SERVER_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions()
    {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(ConfigOptions.key("adapter").stringType().defaultValue("LogMiner"));
        options.add(ConfigOptions.key("history-file").stringType().defaultValue("history.txt"));
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context)
    {
        // 辅助工具，用于获取和验证配置
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        // 验证所有选项
        helper.validate();

        // 获取表结构
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        String database = config.get(DATABASE);
        String table = config.get(TABLE);

        // 创建 Debezium 属性
        Properties properties = new Properties();
        properties.setProperty("database.hostname", config.get(HOSTNAME));
        properties.setProperty("database.port", String.valueOf(config.get(PORT)));
        properties.setProperty("database.user", config.get(USERNAME));
        properties.setProperty("database.password", config.get(PASSWORD));
        properties.setProperty("database.dbname", database);
        properties.setProperty("database.server.id", config.get(SERVER_ID));
        properties.setProperty("database.server.name", config.get(SERVER_NAME));
        properties.setProperty("table.include.list", database + "\\." + table);

        // 默认设置
        properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", "history-" + database + "-" + table + ".txt");
        properties.setProperty("database.connection.adapter", "LogMiner");
        properties.setProperty("database.serverTimezone", "UTC");

        // 从表选项中获取所有以 'debezium.' 开头的属性
        context.getCatalogTable().getOptions().forEach((key, value) -> {
            if (key.startsWith("debezium.")) {
                properties.setProperty(key, value);
            }
        });

        RowDataDebeziumDeserializeSchema deserializer = RowDataDebeziumDeserializeSchema.newBuilder()
                .setPhysicalRowType((RowType) schema.toPhysicalRowDataType().getLogicalType())
                .setResultTypeInfo(InternalTypeInfo.of(schema.toPhysicalRowDataType().getLogicalType()))
                .build();

        // 创建 Dameng CDC 源
        DebeziumSourceFunction<RowData> sourceFunction = DamengSource.create(
                deserializer,
                properties
        );

        return new DamengTableSource(
                schema.toPhysicalRowDataType(),
                sourceFunction
        );
    }

    /**
     * Dameng 表源实现
     */
    private static class DamengTableSource
            implements ScanTableSource
    {
        private final DataType outputType;
        private final DebeziumSourceFunction<RowData> sourceFunction;

        public DamengTableSource(
                DataType outputType,
                DebeziumSourceFunction<RowData> sourceFunction)
        {
            this.outputType = outputType;
            this.sourceFunction = sourceFunction;
        }

        @Override
        public ChangelogMode getChangelogMode()
        {
            return ChangelogMode.all();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext)
        {
            return SourceFunctionProvider.of(sourceFunction, false);
        }

        @Override
        public DynamicTableSource copy()
        {
            return new DamengTableSource(outputType, sourceFunction);
        }

        @Override
        public String asSummaryString()
        {
            return "Dameng-CDC";
        }
    }
}
