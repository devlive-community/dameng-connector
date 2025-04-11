package org.devlive.connector;

import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.devlive.connector.dameng.DamengConnector;

import java.util.Properties;

public class DamengSource
{
    private DamengSource() {}

    /**
     * 创建一个 Dameng CDC 源函数
     *
     * @param <T> 反序列化的目标类型
     * @param deserializer 用于将 Debezium 更改事件转换为所需类型的反序列化器
     * @param properties Debezium 连接器配置
     * @return Debezium 源函数
     */
    public static <T> DebeziumSourceFunction<T> create(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties)
    {
        properties.setProperty("connector.class", DamengConnector.class.getName());

        return new DebeziumSourceFunction<>(
                deserializer,
                properties,
                null,
                new DamengValidator(properties)
        );
    }

    /**
     * 创建一个 Dameng CDC 源函数，使用 JSON 反序列化
     *
     * @param properties Debezium 连接器配置
     * @return Debezium JSON 源函数
     */
    public static DebeziumSourceFunction<String> createJson(Properties properties)
    {
        return create(new JsonDebeziumDeserializationSchema(), properties);
    }

    /**
     * 获取 Dameng CDC 源构建器
     *
     * @param <T> 输出记录类型
     * @return 构建器实例
     */
    public static <T> Builder<T> builder()
    {
        return new Builder<>();
    }

    /**
     * Dameng 源构建器
     *
     * @param <T> 输出记录类型
     */
    public static class Builder<T>
    {
        private final Properties properties = new Properties();
        private DebeziumDeserializationSchema<T> deserializer;

        /**
         * 设置 Dameng 主机名
         */
        public Builder<T> hostname(String hostname)
        {
            properties.setProperty("database.hostname", hostname);
            return this;
        }

        /**
         * 设置 Dameng 端口
         */
        public Builder<T> port(int port)
        {
            properties.setProperty("database.port", String.valueOf(port));
            return this;
        }

        /**
         * 设置 Dameng 用户名
         */
        public Builder<T> username(String username)
        {
            properties.setProperty("database.user", username);
            return this;
        }

        /**
         * 设置 Dameng 密码
         */
        public Builder<T> password(String password)
        {
            properties.setProperty("database.password", password);
            return this;
        }

        /**
         * 设置 Dameng 数据库名
         */
        public Builder<T> database(String database)
        {
            properties.setProperty("database.dbname", database);
            return this;
        }

        /**
         * 设置要监控的表列表
         */
        public Builder<T> tableIncludeList(String tableIncludeList)
        {
            properties.setProperty("table.include.list", tableIncludeList);
            return this;
        }

        /**
         * 设置服务器ID
         */
        public Builder<T> serverId(String serverId)
        {
            properties.setProperty("database.server.id", serverId);
            return this;
        }

        /**
         * 设置服务器名称
         */
        public Builder<T> serverName(String serverName)
        {
            properties.setProperty("database.server.name", serverName);
            return this;
        }

        /**
         * 设置数据库历史文件
         */
        public Builder<T> historyFile(String historyFile)
        {
            properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
            properties.setProperty("database.history.file.filename", historyFile);
            return this;
        }

        /**
         * 设置偏移量存储
         */
        public Builder<T> offsetStorage(String offsetFile)
        {
            properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
            properties.setProperty("offset.storage.file.filename", offsetFile);
            properties.setProperty("offset.flush.interval.ms", "60000");
            return this;
        }

        /**
         * 设置连接适配器类型
         */
        public Builder<T> adapter(String adapter)
        {
            properties.setProperty("database.connection.adapter", adapter);
            return this;
        }

        /**
         * 设置记录挖掘策略
         */
        public Builder<T> miningStrategy(String strategy)
        {
            properties.setProperty("debezium.log.mining.strategy", strategy);
            return this;
        }

        /**
         * 设置连续挖掘
         */
        public Builder<T> continuousMine(boolean continuousMine)
        {
            properties.setProperty("debezium.log.mining.continuous.mine", String.valueOf(continuousMine));
            return this;
        }

        /**
         * 设置任何自定义属性
         */
        public Builder<T> property(String key, String value)
        {
            properties.setProperty(key, value);
            return this;
        }

        /**
         * 设置反序列化器
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer)
        {
            this.deserializer = deserializer;
            return this;
        }

        /**
         * 构建 DebeziumSourceFunction
         */
        public DebeziumSourceFunction<T> build()
        {
            if (deserializer == null) {
                throw new IllegalArgumentException("Must specify a deserializer");
            }

            // 设置默认属性（如果尚未设置）
            if (!properties.containsKey("database.connection.adapter")) {
                properties.setProperty("database.connection.adapter", "LogMiner");
            }

            if (!properties.containsKey("database.serverTimezone")) {
                properties.setProperty("database.serverTimezone", "UTC");
            }

            if (!properties.containsKey("database.history")) {
                properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
                properties.setProperty("database.history.file.filename", "history.txt");
            }

            // 设置连接器类
            properties.setProperty("connector.class", DamengConnector.class.getName());

            return new DebeziumSourceFunction<>(
                    deserializer,
                    properties,
                    null,
                    new DamengValidator(properties)
            );
        }
    }
}
