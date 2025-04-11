/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.data.Envelope;
import org.devlive.connector.dameng.Scn;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

/**
 * This class holds one parsed DML LogMiner record details
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class LogMinerDmlEntryImpl
        implements LogMinerDmlEntry
{
    private final Envelope.Operation commandType;
    private final List<LogMinerColumnValue> newLmColumnValues;
    private final List<LogMinerColumnValue> oldLmColumnValues;
    private String objectOwner;
    private String objectName;
    private Timestamp sourceTime;
    private String transactionId;
    private Scn scn;
    private String rowId;

    public LogMinerDmlEntryImpl(Envelope.Operation commandType, List<LogMinerColumnValue> newLmColumnValues, List<LogMinerColumnValue> oldLmColumnValues)
    {
        this.commandType = commandType;
        this.newLmColumnValues = newLmColumnValues;
        this.oldLmColumnValues = oldLmColumnValues;
    }

    @Override
    public Envelope.Operation getCommandType()
    {
        return commandType;
    }

    @Override
    public List<LogMinerColumnValue> getOldValues()
    {
        return oldLmColumnValues;
    }

    @Override
    public List<LogMinerColumnValue> getNewValues()
    {
        return newLmColumnValues;
    }

    @Override
    public String getTransactionId()
    {
        return transactionId;
    }

    @Override
    public void setTransactionId(String id)
    {
        this.transactionId = id;
    }

    @Override
    public String getObjectOwner()
    {
        return objectOwner;
    }

    @Override
    public void setObjectOwner(String name)
    {
        this.objectOwner = name;
    }

    @Override
    public String getObjectName()
    {
        return objectName;
    }

    @Override
    public void setObjectName(String name)
    {
        this.objectName = name;
    }

    @Override
    public Timestamp getSourceTime()
    {
        return sourceTime;
    }

    @Override
    public void setSourceTime(Timestamp changeTime)
    {
        this.sourceTime = changeTime;
    }

    @Override
    public String getRowId()
    {
        return rowId;
    }

    @Override
    public void setRowId(String rowId)
    {
        this.rowId = rowId;
    }

    @Override
    public Scn getScn()
    {
        return scn;
    }

    @Override
    public void setScn(Scn scn)
    {
        this.scn = scn;
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
        LogMinerDmlEntryImpl that = (LogMinerDmlEntryImpl) o;
        return commandType == that.commandType &&
                Objects.equals(newLmColumnValues, that.newLmColumnValues) &&
                Objects.equals(oldLmColumnValues, that.oldLmColumnValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandType, newLmColumnValues, oldLmColumnValues);
    }
}
