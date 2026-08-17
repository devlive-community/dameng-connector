/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.parser;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerColumnValue;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link LogMinerDmlParser}, focused on the WHERE-clause (before image) parsing that
 * regressed in issue #10 where only the primary key was captured.
 */
public class LogMinerDmlParserTest
{
    private static final String TX_ID = "0000000000001966C";

    private final LogMinerDmlParser parser = new LogMinerDmlParser();

    private static Table table()
    {
        return Table.editor()
                .tableId(new TableId("TEST", "TEST", "T00003"))
                .addColumn(Column.editor().name("ID").position(1).create())
                .addColumn(Column.editor().name("NAME").position(2).create())
                .addColumn(Column.editor().name("AGE").position(3).create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    private static Map<String, Object> toMap(List<LogMinerColumnValue> values)
    {
        Map<String, Object> result = new LinkedHashMap<>();
        for (LogMinerColumnValue value : values) {
            result.put(value.getColumnName(), value.getColumnData());
        }
        return result;
    }

    @Test
    public void updateShouldCaptureEveryColumnInTheBeforeImage()
    {
        // Only NAME changes; the before image (WHERE clause) contains all columns thanks to
        // ALL-column supplemental logging.
        final String sql = "UPDATE \"TEST\".\"T00003\" SET \"NAME\" = 'fff' "
                + "WHERE \"ID\" = 14 AND \"NAME\" = 'aaa' AND \"AGE\" = 25;";

        LogMinerDmlEntry entry = parser.parse(sql, table(), TX_ID);

        Map<String, Object> before = toMap(entry.getOldValues());
        assertEquals("14", before.get("ID"));
        assertEquals("aaa", before.get("NAME"));
        assertEquals("25", before.get("AGE"));

        Map<String, Object> after = toMap(entry.getNewValues());
        assertEquals("14", after.get("ID"));
        assertEquals("fff", after.get("NAME"));
        // Unchanged columns fall back to the before image and must not be lost.
        assertEquals("25", after.get("AGE"));
    }

    @Test
    public void deleteShouldCaptureEveryColumnInTheBeforeImage()
    {
        final String sql = "DELETE FROM \"TEST\".\"T00003\" "
                + "WHERE \"ID\" = 14 AND \"NAME\" = 'aaa' AND \"AGE\" = 25;";

        LogMinerDmlEntry entry = parser.parse(sql, table(), TX_ID);

        Map<String, Object> before = toMap(entry.getOldValues());
        assertEquals("14", before.get("ID"));
        assertEquals("aaa", before.get("NAME"));
        assertEquals("25", before.get("AGE"));
    }

    @Test
    public void whereClauseShouldHandleOrConditions()
    {
        final String sql = "DELETE FROM \"TEST\".\"T00003\" "
                + "WHERE \"ID\" = 14 OR \"NAME\" = 'aaa' OR \"AGE\" = 25;";

        LogMinerDmlEntry entry = parser.parse(sql, table(), TX_ID);

        Map<String, Object> before = toMap(entry.getOldValues());
        assertEquals("14", before.get("ID"));
        assertEquals("aaa", before.get("NAME"));
        assertEquals("25", before.get("AGE"));
    }

    @Test
    public void whereClauseShouldHandleIsNull()
    {
        final String sql = "DELETE FROM \"TEST\".\"T00003\" "
                + "WHERE \"ID\" = 14 AND \"NAME\" IS NULL AND \"AGE\" = 25;";

        LogMinerDmlEntry entry = parser.parse(sql, table(), TX_ID);

        Map<String, Object> before = toMap(entry.getOldValues());
        assertEquals("14", before.get("ID"));
        assertNull(before.get("NAME"));
        assertEquals("25", before.get("AGE"));
    }
}
