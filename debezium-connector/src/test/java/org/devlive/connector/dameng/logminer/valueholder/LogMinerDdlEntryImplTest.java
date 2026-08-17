/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.valueholder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests command-type resolution used to route DDL to CREATE/ALTER/DROP schema change events
 * (issues #12 and the ALTER/DROP support).
 */
public class LogMinerDdlEntryImplTest
{
    @Test
    public void resolvesCreateTable()
    {
        assertEquals("CREATE TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("CREATE TABLE \"TEST\".\"T00003\" (\"ID\" NUMBER)").getCommandType());
    }

    @Test
    public void resolvesCreateTableIgnoringCaseAndWhitespace()
    {
        assertEquals("CREATE TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("  create   table  x (id number)").getCommandType());
    }

    @Test
    public void resolvesCreateGlobalTemporaryTable()
    {
        assertEquals("CREATE TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("CREATE GLOBAL TEMPORARY TABLE \"T\" (\"ID\" NUMBER)").getCommandType());
    }

    @Test
    public void resolvesAlterTable()
    {
        assertEquals("ALTER TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("ALTER TABLE \"TEST\".\"T00003\" ADD (\"C\" NUMBER)").getCommandType());
    }

    @Test
    public void resolvesDropTable()
    {
        assertEquals("DROP TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("DROP TABLE \"TEST\".\"T00003\"").getCommandType());
    }

    @Test
    public void resolvesTruncateTable()
    {
        assertEquals("TRUNCATE TABLE",
                LogMinerDdlEntryImpl.fromRedoSql("TRUNCATE TABLE \"TEST\".\"T00003\"").getCommandType());
    }

    @Test
    public void preservesTheOriginalDdlText()
    {
        String sql = "CREATE TABLE \"TEST\".\"T00003\" (\"ID\" NUMBER)";
        assertEquals(sql, LogMinerDdlEntryImpl.fromRedoSql(sql).getDdlText());
    }
}
