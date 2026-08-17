/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Guards against an unresolved {@code ${project.version}} placeholder leaking from build.version,
 * which broke change-event metadata and crashed debezium-server with a StackOverflowError (issue #9).
 */
public class ModuleTest
{
    @Test
    public void versionShouldBeResolvedAndNotAPlaceholder()
    {
        String version = Module.version();
        assertNotNull(version);
        assertFalse("version must not be blank", version.trim().isEmpty());
        assertFalse("version must not contain an unresolved ${...} placeholder", version.contains("${"));
    }

    @Test
    public void nameAndContextNameShouldBeStable()
    {
        assertTrue("dameng".equals(Module.name()));
        assertTrue("Dameng".equals(Module.contextName()));
    }
}
