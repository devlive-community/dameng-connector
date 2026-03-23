package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

import java.util.Properties;

/**
 * 达梦数据库 Flink CDC Pipeline DataSource 实现。
 * 通过 Debezium 捕获变更数据，并以 Pipeline Event 的形式发射。
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class DamengPipelineSource
        implements DataSource
{
    private final Properties debeziumProperties;

    public DamengPipelineSource(Properties debeziumProperties)
    {
        this.debeziumProperties = debeziumProperties;
    }

    @Override
    public EventSourceProvider getEventSourceProvider()
    {
        return FlinkSourceProvider.of(new DamengEventFlinkSource(debeziumProperties));
    }

    @Override
    public MetadataAccessor getMetadataAccessor()
    {
        return new DamengMetadataAccessor(debeziumProperties);
    }
}
