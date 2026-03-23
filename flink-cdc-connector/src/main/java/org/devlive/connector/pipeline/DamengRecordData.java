package org.devlive.connector.pipeline;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 基于 Map 的 RecordData 实现，用于将 Debezium JSON 记录转换为 Pipeline 事件。
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "DM_DEFAULT_ENCODING"})
public class DamengRecordData
        implements RecordData
{
    private final Map<String, Object> data;
    private final List<Column> columns;

    public DamengRecordData(Map<String, Object> data, Schema schema)
    {
        this.data = data;
        this.columns = schema.getColumns();
    }

    @Override
    public int getArity()
    {
        return columns.size();
    }

    @Override
    public boolean isNullAt(int pos)
    {
        return data.get(columns.get(pos).getName()) == null;
    }

    @Override
    public boolean getBoolean(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        return Boolean.parseBoolean(String.valueOf(val));
    }

    @Override
    public byte getByte(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).byteValue();
        }
        return Byte.parseByte(String.valueOf(val));
    }

    @Override
    public short getShort(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return Short.parseShort(String.valueOf(val));
    }

    @Override
    public int getInt(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        return Integer.parseInt(String.valueOf(val));
    }

    @Override
    public long getLong(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        return Long.parseLong(String.valueOf(val));
    }

    @Override
    public float getFloat(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).floatValue();
        }
        return Float.parseFloat(String.valueOf(val));
    }

    @Override
    public double getDouble(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return Double.parseDouble(String.valueOf(val));
    }

    @Override
    public StringData getString(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val == null) {
            return null;
        }
        return BinaryStringData.fromString(String.valueOf(val));
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val == null) {
            return null;
        }
        BigDecimal decimal;
        if (val instanceof BigDecimal) {
            decimal = (BigDecimal) val;
        }
        else if (val instanceof Number) {
            decimal = new BigDecimal(val.toString());
        }
        else {
            decimal = new BigDecimal(String.valueOf(val));
        }
        return DecimalData.fromBigDecimal(decimal, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val == null) {
            return null;
        }
        if (val instanceof Long) {
            return TimestampData.fromMillis((Long) val);
        }
        return TimestampData.fromMillis(Long.parseLong(String.valueOf(val)));
    }

    @Override
    public ZonedTimestampData getZonedTimestamp(int pos, int precision)
    {
        return null;
    }

    @Override
    public LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision)
    {
        return null;
    }

    @Override
    public byte[] getBinary(int pos)
    {
        Object val = data.get(columns.get(pos).getName());
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val == null) {
            return null;
        }
        return String.valueOf(val).getBytes();
    }

    @Override
    public ArrayData getArray(int pos)
    {
        return null;
    }

    @Override
    public MapData getMap(int pos)
    {
        return null;
    }

    @Override
    public RecordData getRow(int pos, int numFields)
    {
        return null;
    }
}
