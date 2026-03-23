package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.types.DataTypes;
import org.devlive.connector.JdbcUrlUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 达梦数据库元数据访问器，通过 JDBC 查询表结构信息。
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class DamengMetadataAccessor
        implements MetadataAccessor
{
    private final Properties properties;

    public DamengMetadataAccessor(Properties properties)
    {
        this.properties = properties;
    }

    @Override
    public List<String> listNamespaces()
    {
        List<String> namespaces = new ArrayList<>();
        String dbName = properties.getProperty("database.dbname");
        if (dbName != null) {
            namespaces.add(dbName);
        }
        return namespaces;
    }

    @Override
    public List<String> listSchemas(String namespace)
    {
        List<String> schemas = new ArrayList<>();
        try (Connection conn = openConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas from DaMeng", e);
        }
        return schemas;
    }

    @Override
    public List<TableId> listTables(String namespace, String schema)
    {
        List<TableId> tables = new ArrayList<>();
        try (Connection conn = openConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    String tableSchema = rs.getString("TABLE_SCHEM");
                    String tableName = rs.getString("TABLE_NAME");
                    tables.add(TableId.tableId(tableSchema, tableName));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list tables from DaMeng", e);
        }
        return tables;
    }

    @Override
    public Schema getTableSchema(TableId tableId)
    {
        Schema.Builder builder = Schema.newBuilder();
        List<String> primaryKeys = new ArrayList<>();

        try (Connection conn = openConnection()) {
            DatabaseMetaData meta = conn.getMetaData();

            // 获取主键
            try (ResultSet pkRs = meta.getPrimaryKeys(null, tableId.getSchemaName(),
                    tableId.getTableName())) {
                while (pkRs.next()) {
                    primaryKeys.add(pkRs.getString("COLUMN_NAME"));
                }
            }

            // 获取列信息
            try (ResultSet colRs = meta.getColumns(null, tableId.getSchemaName(),
                    tableId.getTableName(), "%")) {
                while (colRs.next()) {
                    String columnName = colRs.getString("COLUMN_NAME");
                    int sqlType = colRs.getInt("DATA_TYPE");
                    int columnSize = colRs.getInt("COLUMN_SIZE");
                    int decimalDigits = colRs.getInt("DECIMAL_DIGITS");
                    boolean nullable = colRs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;

                    org.apache.flink.cdc.common.types.DataType dataType =
                            sqlTypeToFlinkCdcType(sqlType, columnSize, decimalDigits, nullable);
                    builder.physicalColumn(columnName, dataType);
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table schema from DaMeng", e);
        }

        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }

        return builder.build();
    }

    private org.apache.flink.cdc.common.types.DataType sqlTypeToFlinkCdcType(
            int sqlType, int columnSize, int decimalDigits, boolean nullable)
    {
        org.apache.flink.cdc.common.types.DataType type;
        switch (sqlType) {
            case java.sql.Types.TINYINT:
                type = DataTypes.TINYINT();
                break;
            case java.sql.Types.SMALLINT:
                type = DataTypes.SMALLINT();
                break;
            case java.sql.Types.INTEGER:
                type = DataTypes.INT();
                break;
            case java.sql.Types.BIGINT:
                type = DataTypes.BIGINT();
                break;
            case java.sql.Types.FLOAT:
                type = DataTypes.FLOAT();
                break;
            case java.sql.Types.DOUBLE:
                type = DataTypes.DOUBLE();
                break;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                type = DataTypes.DECIMAL(columnSize, decimalDigits);
                break;
            case java.sql.Types.CHAR:
                type = DataTypes.CHAR(columnSize);
                break;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
                type = DataTypes.VARCHAR(columnSize);
                break;
            case java.sql.Types.DATE:
                type = DataTypes.DATE();
                break;
            case java.sql.Types.TIMESTAMP:
                type = DataTypes.TIMESTAMP(6);
                break;
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                type = DataTypes.BOOLEAN();
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                type = DataTypes.BYTES();
                break;
            default:
                type = DataTypes.STRING();
                break;
        }
        return nullable ? type.nullable() : type.notNull();
    }

    private Connection openConnection() throws SQLException
    {
        DriverManager.registerDriver(new dm.jdbc.driver.DmDriver());
        String url = JdbcUrlUtils.getConnectionUrlWithSid(properties);
        String user = properties.getProperty("database.user");
        String password = properties.getProperty("database.password");
        return DriverManager.getConnection(url, user, password);
    }
}
