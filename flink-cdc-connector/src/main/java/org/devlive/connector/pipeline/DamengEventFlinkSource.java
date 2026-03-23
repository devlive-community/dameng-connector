package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * 基于 Flink FLIP-27 Source API 的达梦 CDC 事件源，用于 Pipeline 模式。
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class DamengEventFlinkSource
        implements Source<Event, DamengPipelineSplit, Long>
{
    private static final long serialVersionUID = 1L;

    private final Properties debeziumProperties;

    public DamengEventFlinkSource(Properties debeziumProperties)
    {
        this.debeziumProperties = debeziumProperties;
    }

    @Override
    public Boundedness getBoundedness()
    {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Event, DamengPipelineSplit> createReader(SourceReaderContext context)
            throws Exception
    {
        return new DamengPipelineSourceReader(debeziumProperties, context);
    }

    @Override
    public SplitEnumerator<DamengPipelineSplit, Long> createEnumerator(
            SplitEnumeratorContext<DamengPipelineSplit> context)
            throws Exception
    {
        return new DamengPipelineSplitEnumerator(context);
    }

    @Override
    public SplitEnumerator<DamengPipelineSplit, Long> restoreEnumerator(
            SplitEnumeratorContext<DamengPipelineSplit> context,
            Long checkpoint)
            throws Exception
    {
        return new DamengPipelineSplitEnumerator(context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<DamengPipelineSplit> getSplitSerializer()
    {
        return new SimpleVersionedSerializer<DamengPipelineSplit>()
        {
            @Override
            public int getVersion()
            {
                return 1;
            }

            @Override
            public byte[] serialize(DamengPipelineSplit split)
            {
                return new byte[0];
            }

            @Override
            public DamengPipelineSplit deserialize(int version, byte[] serialized)
            {
                return new DamengPipelineSplit();
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer()
    {
        return new SimpleVersionedSerializer<Long>()
        {
            @Override
            public int getVersion()
            {
                return 1;
            }

            @Override
            public byte[] serialize(Long checkpoint) throws IOException
            {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(checkpoint);
                return buffer.array();
            }

            @Override
            public Long deserialize(int version, byte[] serialized) throws IOException
            {
                return ByteBuffer.wrap(serialized).getLong();
            }
        };
    }
}
