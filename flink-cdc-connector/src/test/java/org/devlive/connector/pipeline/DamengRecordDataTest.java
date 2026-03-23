package org.devlive.connector.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.cdc.common.schema.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DamengRecordDataTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Schema schema;

    @Before
    public void setup() throws Exception
    {
        String fieldsJson = "["
                + "{\"field\":\"id\",\"type\":\"int32\",\"optional\":false},"
                + "{\"field\":\"name\",\"type\":\"string\",\"optional\":true},"
                + "{\"field\":\"age\",\"type\":\"int64\",\"optional\":true},"
                + "{\"field\":\"score\",\"type\":\"float64\",\"optional\":true},"
                + "{\"field\":\"active\",\"type\":\"boolean\",\"optional\":true}"
                + "]";
        JsonNode fields = MAPPER.readTree(fieldsJson);
        schema = DamengSchemaConverter.fromDebeziumSchema(fields, Collections.singletonList("id"));
    }

    @Test
    public void testArity()
    {
        Map<String, Object> data = new HashMap<>();
        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals(5, record.getArity());
    }

    @Test
    public void testIsNullAt()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("name", null);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertFalse(record.isNullAt(0));  // id = 1
        assertTrue(record.isNullAt(1));   // name = null
        assertTrue(record.isNullAt(2));   // age not set
    }

    @Test
    public void testGetInt()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("id", 42);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals(42, record.getInt(0));
    }

    @Test
    public void testGetIntFromString()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "123");

        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals(123, record.getInt(0));
    }

    @Test
    public void testGetLong()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("age", 99999999999L);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals(99999999999L, record.getLong(2));
    }

    @Test
    public void testGetString()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "Alice");

        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals("Alice", record.getString(1).toString());
    }

    @Test
    public void testGetStringNull()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("name", null);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertNull(record.getString(1));
    }

    @Test
    public void testGetDouble()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("score", 98.5);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertEquals(98.5, record.getDouble(3), 0.001);
    }

    @Test
    public void testGetBoolean()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("active", true);

        DamengRecordData record = new DamengRecordData(data, schema);
        assertTrue(record.getBoolean(4));
    }

    @Test
    public void testGetBooleanFromString()
    {
        Map<String, Object> data = new HashMap<>();
        data.put("active", "true");

        DamengRecordData record = new DamengRecordData(data, schema);
        assertTrue(record.getBoolean(4));
    }
}
