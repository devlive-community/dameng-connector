/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link Scn}, the null-aware System Change Number value type used for offsets.
 */
public class ScnTest
{
    @Test
    public void valueOfAndLongValue()
    {
        assertEquals(5L, Scn.valueOf(5).longValue());
        assertEquals(5L, Scn.valueOf(5L).longValue());
        assertEquals(5L, Scn.valueOf("5").longValue());
    }

    @Test
    public void nullScnReportsNullAndZeroLong()
    {
        assertTrue(Scn.NULL.isNull());
        assertEquals(0L, Scn.NULL.longValue());
        assertFalse(Scn.valueOf(1).isNull());
    }

    @Test
    public void compareToTreatsNullAsSmallest()
    {
        assertTrue(Scn.valueOf(5).compareTo(Scn.valueOf(10)) < 0);
        assertTrue(Scn.valueOf(10).compareTo(Scn.valueOf(5)) > 0);
        assertEquals(0, Scn.valueOf(5).compareTo(Scn.valueOf(5)));
        assertEquals(0, Scn.NULL.compareTo(Scn.NULL));
        assertTrue(Scn.NULL.compareTo(Scn.valueOf(5)) < 0);
        assertTrue(Scn.valueOf(5).compareTo(Scn.NULL) > 0);
    }

    @Test
    public void addHandlesNullOperands()
    {
        assertEquals(Scn.valueOf(8), Scn.valueOf(5).add(Scn.valueOf(3)));
        assertEquals(Scn.valueOf(3), Scn.NULL.add(Scn.valueOf(3)));
        assertEquals(Scn.valueOf(5), Scn.valueOf(5).add(Scn.NULL));
        assertTrue(Scn.NULL.add(Scn.NULL).isNull());
    }

    @Test
    public void subtractHandlesNullOperands()
    {
        assertEquals(Scn.valueOf(2), Scn.valueOf(5).subtract(Scn.valueOf(3)));
        assertEquals(Scn.valueOf(-3), Scn.NULL.subtract(Scn.valueOf(3)));
        assertEquals(Scn.valueOf(5), Scn.valueOf(5).subtract(Scn.NULL));
        assertTrue(Scn.NULL.subtract(Scn.NULL).isNull());
    }

    @Test
    public void equalsHashCodeAndToString()
    {
        assertEquals(Scn.valueOf(7), Scn.valueOf(7));
        assertEquals(Scn.valueOf(7).hashCode(), Scn.valueOf(7).hashCode());
        assertFalse(Scn.valueOf(7).equals(Scn.valueOf(8)));
        assertEquals("7", Scn.valueOf(7).toString());
        assertEquals("null", Scn.NULL.toString());
    }
}
