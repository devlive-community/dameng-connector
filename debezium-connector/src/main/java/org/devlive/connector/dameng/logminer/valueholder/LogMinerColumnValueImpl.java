/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

import org.devlive.connector.dameng.antlr.listener.ParserUtils;

import java.util.Objects;

/**
 * This class stores parsed column info
 */
public class LogMinerColumnValueImpl
        implements LogMinerColumnValue
{
    private final String columnName;
    private final int columnType;
    private Object columnData;

    public LogMinerColumnValueImpl(String columnName, int columnType)
    {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    @Override
    public Object getColumnData()
    {
        return columnData;
    }

    @Override
    public void setColumnData(Object columnData)
    {
        if (columnData instanceof String) {
            this.columnData = ParserUtils.replaceDoubleBackSlashes((String) columnData);
        }
        else {
            this.columnData = columnData;
        }
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogMinerColumnValueImpl that = (LogMinerColumnValueImpl) o;
        return columnType == that.columnType &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnData, that.columnData);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnData, columnType);
    }
}
