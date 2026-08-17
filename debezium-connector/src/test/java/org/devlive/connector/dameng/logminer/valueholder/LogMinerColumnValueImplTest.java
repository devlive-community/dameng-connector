/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LogMinerColumnValueImpl}.
 */
public class LogMinerColumnValueImplTest
{
    @Test
    public void keepsNameAndType()
    {
        LogMinerColumnValueImpl value = new LogMinerColumnValueImpl("ID", Types.NUMERIC);
        assertEquals("ID", value.getColumnName());
    }

    @Test
    public void stringDataHasDoubleBackslashesCollapsed()
    {
        LogMinerColumnValueImpl value = new LogMinerColumnValueImpl("NAME", Types.VARCHAR);
        value.setColumnData("a\\\\b");
        assertEquals("a\\b", value.getColumnData());
    }

    @Test
    public void nonStringDataIsStoredAsIs()
    {
        LogMinerColumnValueImpl value = new LogMinerColumnValueImpl("AGE", Types.NUMERIC);
        value.setColumnData(25);
        assertEquals(25, value.getColumnData());
    }

    @Test
    public void equalsAndHashCodeConsiderNameTypeAndData()
    {
        LogMinerColumnValueImpl a = new LogMinerColumnValueImpl("ID", Types.NUMERIC);
        a.setColumnData("1");
        LogMinerColumnValueImpl b = new LogMinerColumnValueImpl("ID", Types.NUMERIC);
        b.setColumnData("1");
        LogMinerColumnValueImpl c = new LogMinerColumnValueImpl("ID", Types.NUMERIC);
        c.setColumnData("2");

        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }
}
