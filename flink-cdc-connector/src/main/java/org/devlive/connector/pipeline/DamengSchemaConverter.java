package org.devlive.connector.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

/**
 * 将 Debezium JSON schema 转换为 Flink CDC Pipeline Schema。
 */
public class DamengSchemaConverter
{
    private DamengSchemaConverter() {}

    /**
     * 从 Debezium JSON 记录的 schema 节点中提取 Pipeline Schema。
     *
     * @param schemaNode Debezium value schema 中 before/after 字段对应的 fields 节点
     * @param primaryKeys 主键列名列表
     * @return Flink CDC Pipeline Schema
     */
    public static Schema fromDebeziumSchema(JsonNode schemaNode, List<String> primaryKeys)
    {
        Schema.Builder builder = Schema.newBuilder();

        if (schemaNode != null && schemaNode.isArray()) {
            for (JsonNode field : schemaNode) {
                String fieldName = field.path("field").asText();
                String fieldType = field.path("type").asText();
                boolean optional = field.path("optional").asBoolean(true);

                DataType dataType = toFlinkCdcType(fieldType, field, optional);
                builder.physicalColumn(fieldName, dataType);
            }
        }

        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }

        return builder.build();
    }

    /**
     * 将 Debezium 类型字符串转换为 Flink CDC DataType。
     */
    public static DataType toFlinkCdcType(String debeziumType, JsonNode fieldNode, boolean optional)
    {
        DataType dataType;
        switch (debeziumType) {
            case "int8":
                dataType = DataTypes.TINYINT();
                break;
            case "int16":
                dataType = DataTypes.SMALLINT();
                break;
            case "int32":
                dataType = DataTypes.INT();
                break;
            case "int64":
                dataType = DataTypes.BIGINT();
                break;
            case "float32":
                dataType = DataTypes.FLOAT();
                break;
            case "float64":
                dataType = DataTypes.DOUBLE();
                break;
            case "boolean":
                dataType = DataTypes.BOOLEAN();
                break;
            case "string":
                dataType = DataTypes.STRING();
                break;
            case "bytes":
                dataType = DataTypes.BYTES();
                break;
            default:
                // struct, array, map -> fallback to STRING
                dataType = DataTypes.STRING();
                break;
        }

        if (optional) {
            return dataType.nullable();
        }
        return dataType.notNull();
    }

    /**
     * 从 Debezium value schema 中找到 before 或 after 字段对应的 fields 节点。
     */
    public static JsonNode extractRowFields(JsonNode valueSchema)
    {
        if (valueSchema == null) {
            return null;
        }
        JsonNode fields = valueSchema.path("fields");
        if (fields.isArray()) {
            for (JsonNode field : fields) {
                String fieldName = field.path("field").asText();
                if ("after".equals(fieldName) || "before".equals(fieldName)) {
                    return field.path("fields");
                }
            }
        }
        return null;
    }

    /**
     * 从 Debezium key schema 中提取主键列名。
     */
    public static List<String> extractPrimaryKeys(JsonNode keySchema)
    {
        List<String> keys = new ArrayList<>();
        if (keySchema == null) {
            return keys;
        }
        JsonNode fields = keySchema.path("fields");
        if (fields.isArray()) {
            for (JsonNode field : fields) {
                keys.add(field.path("field").asText());
            }
        }
        return keys;
    }
}
