package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.pipeline.spi.Partition;

import java.io.Serializable;
import java.util.Map;

@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class MapBackedPartition
        implements Partition, Serializable
{
    private static final long serialVersionUID = 1L;
    private final Map<String, String> sourcePartition;

    @SuppressWarnings("unchecked")
    public MapBackedPartition(Map<String, ?> sourcePartition)
    {
        this.sourcePartition = (Map<String, String>) sourcePartition;
    }

    @Override
    public Map<String, String> getSourcePartition()
    {
        return sourcePartition;
    }
}
