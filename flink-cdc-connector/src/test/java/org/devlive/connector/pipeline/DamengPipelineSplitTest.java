package org.devlive.connector.pipeline;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DamengPipelineSplitTest
{
    @Test
    public void testSplitId()
    {
        DamengPipelineSplit split = new DamengPipelineSplit();
        assertEquals(DamengPipelineSplit.SPLIT_ID, split.splitId());
    }

    @Test
    public void testToString()
    {
        DamengPipelineSplit split = new DamengPipelineSplit();
        assertNotNull(split.toString());
        org.junit.Assert.assertTrue(split.toString().contains(DamengPipelineSplit.SPLIT_ID));
    }

    @Test
    public void testSplitSerializer() throws IOException
    {
        DamengEventFlinkSource source = new DamengEventFlinkSource(new Properties());
        SimpleVersionedSerializer<DamengPipelineSplit> serializer = source.getSplitSerializer();

        DamengPipelineSplit original = new DamengPipelineSplit();
        byte[] bytes = serializer.serialize(original);
        DamengPipelineSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertNotNull(restored);
        assertEquals(original.splitId(), restored.splitId());
    }

    @Test
    public void testEnumeratorCheckpointSerializer() throws IOException
    {
        DamengEventFlinkSource source = new DamengEventFlinkSource(new Properties());
        SimpleVersionedSerializer<Long> serializer = source.getEnumeratorCheckpointSerializer();

        long checkpoint = 12345678L;
        byte[] bytes = serializer.serialize(checkpoint);
        long restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(checkpoint, restored);
    }
}
