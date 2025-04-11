/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.BaseChangeRecordEmitter;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerColumnValue;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;

import java.util.Arrays;
import java.util.List;

/**
 * Emits change record based on a single {@link LogMinerDmlEntry} event.
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class LogMinerChangeRecordEmitter<P extends Partition>
        extends BaseChangeRecordEmitter<LogMinerColumnValue, P>
{
    protected final Table table;
    private final LogMinerDmlEntry dmlEntry;

    public LogMinerChangeRecordEmitter(
            P partition,
            OffsetContext offset,
            LogMinerDmlEntry dmlEntry,
            Table table,
            Clock clock
    )
    {
        super(partition, offset, table, clock);
        this.dmlEntry = dmlEntry;
        this.table = table;
    }

    @Override
    public Operation getOperation()
    {
        return dmlEntry.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues()
    {
        List<LogMinerColumnValue> valueList = dmlEntry.getOldValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected Object[] getNewColumnValues()
    {
        List<LogMinerColumnValue> valueList = dmlEntry.getNewValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected String getColumnName(LogMinerColumnValue columnValue)
    {
        return columnValue.getColumnName();
    }

    @Override
    protected Object getColumnData(LogMinerColumnValue columnValue)
    {
        return columnValue.getColumnData();
    }
}
