/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

import io.debezium.data.Envelope;
import org.devlive.connector.dameng.Scn;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LogMinerDmlEntryImpl}.
 */
public class LogMinerDmlEntryImplTest
{
    private static LogMinerColumnValue column(String name, Object data)
    {
        LogMinerColumnValueImpl value = new LogMinerColumnValueImpl(name, 0);
        value.setColumnData(data);
        return value;
    }

    @Test
    public void exposesCommandTypeAndValues()
    {
        List<LogMinerColumnValue> newValues = Collections.singletonList(column("ID", "1"));
        LogMinerDmlEntryImpl entry = new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, newValues, Collections.emptyList());

        assertEquals(Envelope.Operation.CREATE, entry.getCommandType());
        assertEquals(newValues, entry.getNewValues());
        assertTrue(entry.getOldValues().isEmpty());
    }

    @Test
    public void metadataSettersRoundTrip()
    {
        LogMinerDmlEntryImpl entry = new LogMinerDmlEntryImpl(Envelope.Operation.UPDATE, Collections.emptyList(), Collections.emptyList());
        Timestamp now = new Timestamp(1_700_000_000_000L);

        entry.setTransactionId("tx1");
        entry.setObjectOwner("TEST");
        entry.setObjectName("T00003");
        entry.setSourceTime(now);
        entry.setScn(Scn.valueOf(138153));
        entry.setRowId("AAA");

        assertEquals("tx1", entry.getTransactionId());
        assertEquals("TEST", entry.getObjectOwner());
        assertEquals("T00003", entry.getObjectName());
        assertEquals(now, entry.getSourceTime());
        assertEquals(Scn.valueOf(138153), entry.getScn());
        assertEquals("AAA", entry.getRowId());
    }

    @Test
    public void equalsIgnoresMetadataAndComparesValues()
    {
        List<LogMinerColumnValue> values = Collections.singletonList(column("ID", "1"));
        LogMinerDmlEntryImpl a = new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, values, Collections.emptyList());
        LogMinerDmlEntryImpl b = new LogMinerDmlEntryImpl(Envelope.Operation.CREATE, values, Collections.emptyList());
        a.setTransactionId("tx1");
        b.setTransactionId("tx2");
        LogMinerDmlEntryImpl different = new LogMinerDmlEntryImpl(Envelope.Operation.DELETE, Collections.emptyList(), values);

        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(different));
    }
}
