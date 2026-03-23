package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressFBWarnings(value = {"JUA_DONT_ASSERT_INSTANCEOF_IN_TESTS"})
public class DamengPipelineSourceTest
{
    private Properties buildTestProperties()
    {
        Properties props = new Properties();
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5236");
        props.setProperty("database.user", "SYSDBA");
        props.setProperty("database.password", "SYSDBAPASS");
        props.setProperty("database.dbname", "TEST");
        props.setProperty("database.server.id", "5001");
        props.setProperty("database.server.name", "dameng-server");
        return props;
    }

    @Test
    public void testPipelineSourceCreation()
    {
        DataSource source = new DamengPipelineSource(buildTestProperties());
        assertNotNull(source);
    }

    @Test
    public void testGetMetadataAccessor()
    {
        DamengPipelineSource source = new DamengPipelineSource(buildTestProperties());
        MetadataAccessor accessor = source.getMetadataAccessor();
        assertNotNull(accessor);
        assertTrue(accessor instanceof DamengMetadataAccessor);
    }

    @Test
    public void testGetEventSourceProvider()
    {
        DamengPipelineSource source = new DamengPipelineSource(buildTestProperties());
        org.apache.flink.cdc.common.source.EventSourceProvider provider =
                source.getEventSourceProvider();
        assertNotNull(provider);
        assertTrue(provider instanceof org.apache.flink.cdc.common.source.FlinkSourceProvider);
    }

    @Test
    public void testFlinkSourceIsCorrectType()
    {
        DamengPipelineSource source = new DamengPipelineSource(buildTestProperties());
        org.apache.flink.cdc.common.source.FlinkSourceProvider provider =
                (org.apache.flink.cdc.common.source.FlinkSourceProvider) source.getEventSourceProvider();
        assertNotNull(provider.getSource());
        assertTrue(provider.getSource() instanceof DamengEventFlinkSource);
    }

    @Test
    public void testFlinkSourceBoundedness()
    {
        DamengEventFlinkSource flinkSource = new DamengEventFlinkSource(buildTestProperties());
        assertEquals(org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED,
                flinkSource.getBoundedness());
    }

    private static void assertEquals(Object expected, Object actual)
    {
        org.junit.Assert.assertEquals(expected, actual);
    }
}
