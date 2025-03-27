/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.Scn;
import io.debezium.jdbc.JdbcConfiguration;

import java.sql.Timestamp;

/**
 * A history recorder implementation that does not do any recording.
 *
 * @author Chris Cranford
 */
public class NeverHistoryRecorder
        implements HistoryRecorder
{
    @Override
    public void prepare(DamengStreamingChangeEventSourceMetrics streamingMetrics, JdbcConfiguration jdbcConfiguration, long retentionHours)
    {
    }

    @Override
    public void record(Scn scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
            String transactionId, int csf, String redoSql)
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void close()
    {
    }
}
