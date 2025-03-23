package org.devlive.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DamengCdcSqlExample
{
    public static void main(String[] args)
            throws Exception
    {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        env.setParallelism(1);

        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 注册 Dameng CDC 源表
        String createTableSql = "CREATE TABLE dameng_source_table (\n" +
                "    NAME STRING,\n" +
                "    AGE BIGINT,\n" +
                "    PRIMARY KEY (NAME) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'dameng-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '5236',\n" +
                "    'username' = 'SYSDBA',\n" +
                "    'password' = 'SYSDBAPASS',\n" +
                "    'database' = 'TEST',\n" +
                "    'table' = 'T00003',\n" +
                "    'server-id' = '5001',\n" +
                "    'server-name' = 'dameng-server'\n" +
                ")";

        tableEnv.executeSql(createTableSql);

        // 执行查询并打印结果
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM dameng_source_table");
        tableResult.print();

        // 执行作业
        env.execute("Dameng CDC SQL Example");
    }
}
