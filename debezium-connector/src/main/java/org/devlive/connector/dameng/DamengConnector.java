/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DamengConnector
        extends RelationalBaseSourceConnector
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnector.class);

    private Map<String, String> properties;

    @Override
    public String version()
    {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props)
    {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass()
    {
        return DamengConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks)
    {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }

        return Collections.singletonList(properties);
    }

    @Override
    public void stop()
    {
    }

    @Override
    public ConfigDef config()
    {
        return DamengConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config)
    {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());

        DamengConnectorConfig connectorConfig = new DamengConnectorConfig(config);
        try (DamengConnection connection = new DamengConnection(connectorConfig.jdbcConfig(), () -> getClass().getClassLoader())) {
            LOGGER.debug("Successfully tested connection for {} with user '{}'", DamengConnection.connectionString(config), connection.username());
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.info("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(), userValue, e);
            hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config)
    {
        return config.validate(DamengConnectorConfig.ALLFIELDS);
    }
}
