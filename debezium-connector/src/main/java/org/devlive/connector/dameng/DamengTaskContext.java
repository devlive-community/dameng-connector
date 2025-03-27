/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.connector.common.CdcSourceTaskContext;

public class DamengTaskContext
        extends CdcSourceTaskContext
{
    public DamengTaskContext(DamengConnectorConfig config, DamengDatabaseSchema schema)
    {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
