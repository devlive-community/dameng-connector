package org.devlive.connector.pipeline;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * 达梦数据库 Pipeline CDC 分片，整个连接使用单一分片。
 */
public class DamengPipelineSplit
        implements SourceSplit, Serializable
{
    private static final long serialVersionUID = 1L;

    public static final String SPLIT_ID = "dameng-pipeline-split";

    @Override
    public String splitId()
    {
        return SPLIT_ID;
    }

    @Override
    public String toString()
    {
        return "DamengPipelineSplit{id='" + SPLIT_ID + "'}";
    }
}
