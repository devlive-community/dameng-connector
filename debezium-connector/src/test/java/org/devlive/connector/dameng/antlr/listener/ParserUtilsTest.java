/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.antlr.listener;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the pure string helpers in {@link ParserUtils}.
 */
public class ParserUtilsTest
{
    @Test
    public void stripeQuotesRemovesSurroundingDoubleQuotes()
    {
        assertEquals("ABC", ParserUtils.stripeQuotes("\"ABC\""));
        assertEquals("ABC", ParserUtils.stripeQuotes("ABC"));
        assertNull(ParserUtils.stripeQuotes(null));
    }

    @Test
    public void stripeAliasRemovesLeadingAlias()
    {
        assertEquals("COL", ParserUtils.stripeAlias("A.COL", "A"));
        assertEquals("COL", ParserUtils.stripeAlias("COL", "A"));
    }

    @Test
    public void removeApostrophesUnquotesAndMapsNull()
    {
        assertEquals("abc", ParserUtils.removeApostrophes("'abc'"));
        assertEquals("abc", ParserUtils.removeApostrophes("abc"));
        assertNull(ParserUtils.removeApostrophes("null"));
        assertNull(ParserUtils.removeApostrophes("NULL"));
    }

    @Test
    public void replaceDoubleBackSlashesCollapsesEscapes()
    {
        // "a\\b" (two backslashes) collapses to "a\b" (one backslash)
        assertEquals("a\\b", ParserUtils.replaceDoubleBackSlashes("a\\\\b"));
        // a single backslash is left untouched
        assertEquals("a\\b", ParserUtils.replaceDoubleBackSlashes("a\\b"));
        assertNull(ParserUtils.replaceDoubleBackSlashes(null));
    }
}
