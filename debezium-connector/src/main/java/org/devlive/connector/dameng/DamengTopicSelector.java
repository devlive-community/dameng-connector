/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class DamengTopicSelector
{
    private DamengTopicSelector() {}

    public static TopicSelector<TableId> defaultSelector(DamengConnectorConfig connectorConfig)
    {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
