package org.devlive.connector.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.cdc.common.schema.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DamengSchemaConverterTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testFromDebeziumSchemaBasic() throws Exception
    {
        String fieldsJson = "["
                + "{\"field\":\"id\",\"type\":\"int32\",\"optional\":false},"
                + "{\"field\":\"name\",\"type\":\"string\",\"optional\":true},"
                + "{\"field\":\"age\",\"type\":\"int64\",\"optional\":true}"
                + "]";

        JsonNode fieldsNode = MAPPER.readTree(fieldsJson);
        List<String> primaryKeys = Collections.singletonList("id");

        Schema schema = DamengSchemaConverter.fromDebeziumSchema(fieldsNode, primaryKeys);

        assertNotNull(schema);
        assertEquals(3, schema.getColumns().size());
        assertEquals("id", schema.getColumns().get(0).getName());
        assertEquals("name", schema.getColumns().get(1).getName());
        assertEquals("age", schema.getColumns().get(2).getName());
        assertEquals(primaryKeys, schema.primaryKeys());
    }

    @Test
    public void testFromDebeziumSchemaWithoutPrimaryKey() throws Exception
    {
        String fieldsJson = "["
                + "{\"field\":\"col1\",\"type\":\"string\",\"optional\":true},"
                + "{\"field\":\"col2\",\"type\":\"int32\",\"optional\":true}"
                + "]";

        JsonNode fieldsNode = MAPPER.readTree(fieldsJson);
        Schema schema = DamengSchemaConverter.fromDebeziumSchema(fieldsNode, Collections.emptyList());

        assertNotNull(schema);
        assertEquals(2, schema.getColumns().size());
        assertEquals(Collections.emptyList(), schema.primaryKeys());
    }

    @Test
    public void testFromDebeziumSchemaWithNullFields()
    {
        Schema schema = DamengSchemaConverter.fromDebeziumSchema(null, Collections.emptyList());
        assertNotNull(schema);
        assertEquals(0, schema.getColumns().size());
    }

    @Test
    public void testTypeConversions() throws Exception
    {
        String fieldsJson = "["
                + "{\"field\":\"f_int8\",\"type\":\"int8\",\"optional\":true},"
                + "{\"field\":\"f_int16\",\"type\":\"int16\",\"optional\":true},"
                + "{\"field\":\"f_int32\",\"type\":\"int32\",\"optional\":true},"
                + "{\"field\":\"f_int64\",\"type\":\"int64\",\"optional\":true},"
                + "{\"field\":\"f_float32\",\"type\":\"float32\",\"optional\":true},"
                + "{\"field\":\"f_float64\",\"type\":\"float64\",\"optional\":true},"
                + "{\"field\":\"f_bool\",\"type\":\"boolean\",\"optional\":true},"
                + "{\"field\":\"f_string\",\"type\":\"string\",\"optional\":true},"
                + "{\"field\":\"f_bytes\",\"type\":\"bytes\",\"optional\":true}"
                + "]";

        JsonNode fieldsNode = MAPPER.readTree(fieldsJson);
        Schema schema = DamengSchemaConverter.fromDebeziumSchema(fieldsNode, Collections.emptyList());

        assertNotNull(schema);
        assertEquals(9, schema.getColumns().size());
    }

    @Test
    public void testExtractPrimaryKeys() throws Exception
    {
        String keySchemaJson = "{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"field\":\"id\",\"type\":\"int32\",\"optional\":false}"
                + "]"
                + "}";

        JsonNode keySchema = MAPPER.readTree(keySchemaJson);
        List<String> keys = DamengSchemaConverter.extractPrimaryKeys(keySchema);

        assertEquals(Collections.singletonList("id"), keys);
    }

    @Test
    public void testExtractPrimaryKeysComposite() throws Exception
    {
        String keySchemaJson = "{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"field\":\"id1\",\"type\":\"int32\",\"optional\":false},"
                + "{\"field\":\"id2\",\"type\":\"string\",\"optional\":false}"
                + "]"
                + "}";

        JsonNode keySchema = MAPPER.readTree(keySchemaJson);
        List<String> keys = DamengSchemaConverter.extractPrimaryKeys(keySchema);

        assertEquals(Arrays.asList("id1", "id2"), keys);
    }

    @Test
    public void testExtractPrimaryKeysNull()
    {
        List<String> keys = DamengSchemaConverter.extractPrimaryKeys(null);
        assertNotNull(keys);
        assertEquals(0, keys.size());
    }

    @Test
    public void testExtractRowFields() throws Exception
    {
        String valueSchemaJson = "{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "  {\"field\":\"before\",\"type\":\"struct\","
                + "   \"fields\":[{\"field\":\"id\",\"type\":\"int32\",\"optional\":false}]},"
                + "  {\"field\":\"after\",\"type\":\"struct\","
                + "   \"fields\":[{\"field\":\"id\",\"type\":\"int32\",\"optional\":false}]},"
                + "  {\"field\":\"op\",\"type\":\"string\",\"optional\":false}"
                + "]"
                + "}";

        JsonNode valueSchema = MAPPER.readTree(valueSchemaJson);
        JsonNode rowFields = DamengSchemaConverter.extractRowFields(valueSchema);

        assertNotNull(rowFields);
        assertEquals(1, rowFields.size());
        assertEquals("id", rowFields.get(0).path("field").asText());
    }
}
