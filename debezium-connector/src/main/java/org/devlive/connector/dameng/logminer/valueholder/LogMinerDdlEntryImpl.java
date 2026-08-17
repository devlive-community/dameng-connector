/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

/**
 * Default {@link LogMinerDdlEntry} implementation that carries the DDL text and its command type.
 */
public class LogMinerDdlEntryImpl
        implements LogMinerDdlEntry
{
    private final String ddlText;
    private final String commandType;

    public LogMinerDdlEntryImpl(String ddlText, String commandType)
    {
        this.ddlText = ddlText;
        this.commandType = commandType;
    }

    /**
     * Builds a DDL entry from a LogMiner redo SQL statement, deriving the command type
     * (e.g. {@code CREATE TABLE}, {@code ALTER TABLE}, {@code DROP TABLE}) from the statement text.
     *
     * @param redoSql the DDL redo SQL; never null
     * @return the DDL entry
     */
    public static LogMinerDdlEntryImpl fromRedoSql(String redoSql)
    {
        return new LogMinerDdlEntryImpl(redoSql, resolveCommandType(redoSql));
    }

    private static String resolveCommandType(String redoSql)
    {
        // Normalise the statement so keyword detection is independent of case and spacing.
        final String normalized = redoSql.trim().replaceAll("\\s+", " ").toUpperCase();
        if (normalized.startsWith("CREATE TABLE") || normalized.startsWith("CREATE GLOBAL TEMPORARY TABLE")) {
            return "CREATE TABLE";
        }
        if (normalized.startsWith("ALTER TABLE")) {
            return "ALTER TABLE";
        }
        if (normalized.startsWith("DROP TABLE")) {
            return "DROP TABLE";
        }
        if (normalized.startsWith("TRUNCATE TABLE")) {
            return "TRUNCATE TABLE";
        }
        // Return the leading keywords so callers can log/skip unsupported statements meaningfully.
        final int firstSplit = normalized.indexOf(' ');
        return firstSplit > 0 ? normalized.substring(0, firstSplit) : normalized;
    }

    @Override
    public String getDdlText()
    {
        return ddlText;
    }

    @Override
    public String getCommandType()
    {
        return commandType;
    }
}
