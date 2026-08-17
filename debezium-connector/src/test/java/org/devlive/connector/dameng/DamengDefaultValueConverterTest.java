/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.relational.Column;
import org.junit.Test;

import java.sql.Types;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the drop-vs-keep decision of {@link DamengDefaultValueConverter} (issue #18). A null value
 * converter is used so only the placeholder/expression handling is exercised (the type conversion of
 * literal values needs a live connection and is covered by integration tests).
 */
public class DamengDefaultValueConverterTest
{
    private final DamengDefaultValueConverter converter = new DamengDefaultValueConverter(null);

    private static Column timestampColumn()
    {
        return Column.editor().name("TS").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).create();
    }

    private static Column varcharColumn()
    {
        return Column.editor().name("NAME").type("VARCHAR2").jdbcType(Types.VARCHAR).length(20).create();
    }

    private static Column charColumn()
    {
        return Column.editor().name("CODE").type("CHAR").jdbcType(Types.CHAR).length(5).create();
    }

    @Test
    public void dropsNullExpression()
    {
        assertFalse(converter.parseDefaultValue(timestampColumn(), null).isPresent());
    }

    @Test
    public void dropsNullKeywordAndBlank()
    {
        assertFalse(converter.parseDefaultValue(varcharColumn(), "NULL").isPresent());
        assertFalse(converter.parseDefaultValue(varcharColumn(), "null").isPresent());
        assertFalse(converter.parseDefaultValue(varcharColumn(), "   ").isPresent());
    }

    @Test
    public void dropsNonEvaluableTemporalFunctions()
    {
        // The exact bug from issue #18: SYSDATE default on a TIMESTAMP column must not be kept.
        assertFalse(converter.parseDefaultValue(timestampColumn(), "SYSDATE").isPresent());
        assertFalse(converter.parseDefaultValue(timestampColumn(), "sysdate").isPresent());
        assertFalse(converter.parseDefaultValue(timestampColumn(), "CURRENT_TIMESTAMP").isPresent());
        assertFalse(converter.parseDefaultValue(timestampColumn(), "LOCALTIMESTAMP").isPresent());
    }

    @Test
    public void keepsAndUnquotesStringLiterals()
    {
        Optional<Object> value = converter.parseDefaultValue(varcharColumn(), "'abc'");
        assertTrue(value.isPresent());
        assertEquals("abc", value.get());
    }

    @Test
    public void padsCharLiteralsToColumnLength()
    {
        Optional<Object> value = converter.parseDefaultValue(charColumn(), "'ab'");
        assertTrue(value.isPresent());
        assertEquals("ab   ", value.get());
    }
}
