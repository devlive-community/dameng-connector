/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.util.IoUtil;

import java.util.Properties;

/**
 * Information about this module.
 *
 * @author Gunnar Morling
 */
public final class Module
{
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "org/devlive/connector/dameng/build.version");

    private Module() {}

    public static String version()
    {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name()
    {
        return "dameng";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName()
    {
        return "Dameng";
    }
}
