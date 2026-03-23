package org.devlive.connector.pipeline;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * 达梦 Pipeline CDC 分片枚举器，将唯一的单一分片分配给第一个读取器。
 */
public class DamengPipelineSplitEnumerator
        implements SplitEnumerator<DamengPipelineSplit, Long>
{
    private static final Logger LOG = LoggerFactory.getLogger(DamengPipelineSplitEnumerator.class);

    private final SplitEnumeratorContext<DamengPipelineSplit> context;
    private boolean splitAssigned;

    public DamengPipelineSplitEnumerator(SplitEnumeratorContext<DamengPipelineSplit> context)
    {
        this.context = context;
        this.splitAssigned = false;
    }

    public DamengPipelineSplitEnumerator(
            SplitEnumeratorContext<DamengPipelineSplit> context,
            Long checkpoint)
    {
        this(context);
    }

    @Override
    public void start()
    {
        // 分片将在 addReader 时分配
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname)
    {
        assignSplitToReader(subtaskId);
    }

    @Override
    public void addSplitsBack(List<DamengPipelineSplit> splits, int subtaskId)
    {
        if (!splits.isEmpty()) {
            LOG.info("Reassigning split back from subtask {}", subtaskId);
            splitAssigned = false;
            assignSplitToReader(subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId)
    {
        assignSplitToReader(subtaskId);
    }

    @Override
    public Long snapshotState(long checkpointId)
    {
        return checkpointId;
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    private void assignSplitToReader(int subtaskId)
    {
        if (!splitAssigned) {
            splitAssigned = true;
            LOG.info("Assigning DamengPipelineSplit to subtask {}", subtaskId);
            context.assignSplit(new DamengPipelineSplit(), subtaskId);
        }
    }
}
