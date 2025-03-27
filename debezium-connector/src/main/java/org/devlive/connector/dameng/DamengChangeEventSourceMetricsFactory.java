/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;

/**
 * @author Chris Cranford
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class DamengChangeEventSourceMetricsFactory<P extends Partition>
        extends DefaultChangeEventSourceMetricsFactory<P>
{
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    public DamengChangeEventSourceMetricsFactory(DamengStreamingChangeEventSourceMetrics streamingMetrics)
    {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(T taskContext,
            ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider eventMetadataProvider)
    {
        return streamingMetrics;
    }
}
