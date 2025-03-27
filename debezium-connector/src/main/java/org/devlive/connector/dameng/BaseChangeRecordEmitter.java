/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Base class to emit change data based on a single entry event.
 */
public abstract class BaseChangeRecordEmitter<T, P extends Partition>
        extends RelationalChangeRecordEmitter<P>
{
    protected final Table table;

    protected BaseChangeRecordEmitter(P partition, OffsetContext offset, Table table, Clock clock)
    {
        super(partition, offset, clock);
        this.table = table;
    }

    public abstract Operation getOperation();

    protected abstract String getColumnName(T columnValue);

    protected abstract Object getColumnData(T columnValue);

    protected Object[] getColumnValues(T[] columnValues)
    {
        Object[] values = new Object[table.columns().size()];

        for (T columnValue : columnValues) {
            int index = table.columnWithName(getColumnName(columnValue)).position() - 1;
            values[index] = getColumnData(columnValue);
        }

        return values;
    }
}
