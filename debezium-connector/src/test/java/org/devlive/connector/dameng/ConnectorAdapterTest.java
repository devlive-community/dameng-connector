/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import org.devlive.connector.dameng.DamengConnectorConfig.ConnectorAdapter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ConnectorAdapter} parsing and connection URL templates.
 */
public class ConnectorAdapterTest
{
    @Test
    public void parseDefaultsToLogMinerForNull()
    {
        assertEquals(ConnectorAdapter.LOG_MINER, ConnectorAdapter.parse(null));
    }

    @Test
    public void parseIsCaseInsensitiveAndTrimmed()
    {
        assertEquals(ConnectorAdapter.LOG_MINER, ConnectorAdapter.parse("logminer"));
        assertEquals(ConnectorAdapter.LOG_MINER, ConnectorAdapter.parse("  LogMiner  "));
        assertEquals(ConnectorAdapter.XSTREAM, ConnectorAdapter.parse("XStream"));
    }

    @Test
    public void parseReturnsNullForUnknownValue()
    {
        assertNull(ConnectorAdapter.parse("bogus"));
    }

    @Test
    public void parseFallsBackToDefaultValueWhenUnknown()
    {
        assertEquals(ConnectorAdapter.LOG_MINER, ConnectorAdapter.parse("bogus", "LogMiner"));
    }

    @Test
    public void connectionUrlUsesPlaceholders()
    {
        assertTrue(ConnectorAdapter.LOG_MINER.getConnectionUrl().startsWith("jdbc:oracle:thin"));
        assertTrue(ConnectorAdapter.LOG_MINER.getConnectionUrl().contains("${hostname}"));
        assertTrue(ConnectorAdapter.XSTREAM.getConnectionUrl().contains("${port}"));
    }
}
