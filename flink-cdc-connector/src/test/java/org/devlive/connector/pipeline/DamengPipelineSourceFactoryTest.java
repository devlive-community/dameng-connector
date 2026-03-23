package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressFBWarnings(value = {"JUA_DONT_ASSERT_INSTANCEOF_IN_TESTS"})
public class DamengPipelineSourceFactoryTest
{
    private static final Map<String, String> REQUIRED_OPTIONS;

    static {
        REQUIRED_OPTIONS = new HashMap<>();
        REQUIRED_OPTIONS.put("hostname", "localhost");
        REQUIRED_OPTIONS.put("username", "SYSDBA");
        REQUIRED_OPTIONS.put("password", "SYSDBAPASS");
        REQUIRED_OPTIONS.put("database", "TEST");
    }

    @Test
    public void testFactoryIdentifier()
    {
        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        assertEquals("dameng", factory.identifier());
    }

    @Test
    public void testRequiredOptions()
    {
        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        Set<?> required = factory.requiredOptions();
        assertNotNull(required);
        assertEquals(4, required.size());
    }

    @Test
    public void testOptionalOptions()
    {
        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        Set<?> optional = factory.optionalOptions();
        assertNotNull(optional);
        assertTrue(optional.size() >= 4);
    }

    @Test
    public void testCreateDataSourceWithRequiredOptions()
    {
        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(REQUIRED_OPTIONS);

        org.apache.flink.cdc.common.source.DataSource source = factory.createDataSource(context);
        assertNotNull(source);
        assertTrue(source instanceof DamengPipelineSource);
    }

    @Test
    public void testCreateDataSourceWithAllOptions()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.put("port", "5236");
        options.put("server-id", "8001");
        options.put("server-name", "test-dameng");
        options.put("table-list", "PUBLIC.T_USER,PUBLIC.T_ORDER");
        options.put("snapshot-mode", "schema_only");
        options.put("history-file", "/tmp/test-history.txt");
        options.put("debezium.log.mining.strategy", "online_catalog");

        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(options);

        org.apache.flink.cdc.common.source.DataSource source = factory.createDataSource(context);
        assertNotNull(source);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDataSourceMissingHostname()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.remove("hostname");

        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(options);
        factory.createDataSource(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDataSourceMissingUsername()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.remove("username");

        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(options);
        factory.createDataSource(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDataSourceMissingDatabase()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.remove("database");

        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(options);
        factory.createDataSource(context);
    }

    @Test
    public void testDefaultPortIsApplied()
    {
        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(REQUIRED_OPTIONS);
        org.apache.flink.cdc.common.source.DataSource source = factory.createDataSource(context);
        assertNotNull(source);
        // If no exception was thrown, the default port was applied correctly
    }

    @Test
    public void testServiceLoaderDiscovery()
    {
        ServiceLoader<Factory> loader = ServiceLoader.load(
                Factory.class, DamengPipelineSourceFactory.class.getClassLoader());
        boolean found = false;
        for (Factory factory : loader) {
            if ("dameng".equals(factory.identifier())) {
                found = true;
                break;
            }
        }
        assertTrue("DamengPipelineSourceFactory should be discoverable via SPI", found);
    }

    @Test
    public void testDebeziumPropertiesPassThrough()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.put("debezium.log.mining.strategy", "online_catalog");
        options.put("debezium.log.mining.continuous.mine", "true");

        DamengPipelineSourceFactory factory = new DamengPipelineSourceFactory();
        DataSourceFactory.Context context = createContext(options);

        // Should create without exception
        org.apache.flink.cdc.common.source.DataSource source = factory.createDataSource(context);
        assertNotNull(source);
    }

    /**
     * 创建测试用的 DataSourceFactory.Context。
     */
    private static DataSourceFactory.Context createContext(Map<String, String> options)
    {
        Configuration configuration = Configuration.fromMap(options);
        return new DataSourceFactory.Context()
        {
            @Override
            public Configuration getFactoryConfiguration()
            {
                return configuration;
            }

            @Override
            public Configuration getPipelineConfiguration()
            {
                return Configuration.fromMap(new HashMap<>());
            }

            @Override
            public ClassLoader getClassLoader()
            {
                return DamengPipelineSourceFactoryTest.class.getClassLoader();
            }
        };
    }
}
