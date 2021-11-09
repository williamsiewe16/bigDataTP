package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;

@Public
@Stable
public class LongTupleWritable implements WritableComparable<LongTupleWritable> {
    private long value1;
    private long value2;

    public LongTupleWritable() {
    }

    public LongTupleWritable(long value1, long value2) {
        this.set(value1, value2);
    }

    public void set(long value1, long value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public Long [] get() {
        return new Long[]{this.value1, this.value2};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.value1);
        dataOutput.writeLong(this.value2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value1 = dataInput.readLong();
        this.value2 = dataInput.readLong();
    }

    @Override
    public int compareTo(LongTupleWritable o) {
        Long thisValue = this.value1;
        Long thatValue = o.value1;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public boolean equals(Object o) {
        if (!(o instanceof LongTupleWritable)) {
            return false;
        } else {
            LongTupleWritable other = (LongTupleWritable)o;
            return (this.value1 == other.value1) & (this.value2 == other.value2);
        }
    }
}
